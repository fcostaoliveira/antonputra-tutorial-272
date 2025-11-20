package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	mon "github.com/antonputra/go-utils/monitoring"
	"github.com/antonputra/go-utils/util"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// LatencyTracker tracks latencies across all workers
type LatencyTracker struct {
	mu        sync.Mutex
	latencies []time.Duration
}

func (lt *LatencyTracker) Add(latency time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.latencies = append(lt.latencies, latency)
}

func (lt *LatencyTracker) GetAndResetP90() float64 {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if len(lt.latencies) == 0 {
		return 0
	}

	// Sort latencies
	sorted := make([]time.Duration, len(lt.latencies))
	copy(sorted, lt.latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	// Calculate p90 index
	p90Index := int(float64(len(sorted)) * 0.90)
	if p90Index >= len(sorted) {
		p90Index = len(sorted) - 1
	}

	p90 := sorted[p90Index].Seconds() * 1000 // Convert to milliseconds

	// Reset latencies for next interval
	lt.latencies = lt.latencies[:0]

	return p90
}

var ctx = context.Background()

// payload for the Redis
type User struct {
	Handle      string `json:"handle"`
	Country     string `json:"country"`
	Timestamp   int64  `json:"timestamp"`
	Description string `json:"description"`
}

// String used to pad to ~512 bytes
const text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui kjdhf 13ye jasd sdhhu2edlka officia deserunt mollit anim id est laborum."

// worker runs a Redis client that performs operations at the target rate
func worker(ctx context.Context, clientID int, addr, password string, targetRate int, pause int, poolSize int, m *mon.Metrics, wg *sync.WaitGroup, totalOps *atomic.Int64, latencyTracker *LatencyTracker) {
	defer wg.Done()

	// Create Redis client for this worker.
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize: poolSize,
	})
	defer rdb.Close()

	// Set a 1-second TTL on the Redis keys.
	expr := time.Duration(time.Duration(1) * time.Second)

	// Initialize the start time and the counter to track the number of operations.
	var start time.Time = time.Now()
	var count int = 0

	// Start an infinite loop and perform the work.
	for {
		// Check if context is cancelled (test duration exceeded)
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Keep track of the time elapsed between each operation.
		end := time.Now()
		elapsed := end.Sub(start)

		// Reset the number of operations each second.
		if elapsed >= time.Second {
			start = time.Now()
			count = 0
		}

		// If the number of operations equals or exceeds the rate, sleep for the remaining time until the next second.
		// Sleeping avoids wasting CPU cycles, allowing for more efficient use of resources.
		if count >= targetRate {
			next := time.Second - elapsed
			if next > time.Nanosecond {
				time.Sleep(next)
			}
		}

		// Use a UUID as the key.
		key := uuid.New()

		// Create a User for the Test.
		u := User{
			Handle:      "@antonvputra",
			Country:     "USA",
			Timestamp:   time.Now().UnixNano(),
			Description: text,
		}

		// Serialize the user to a string.
		value, err := json.Marshal(u)
		if err != nil {
			util.Warn(err, "rdb.Set failed")
			continue
		}
		startSetGet := time.Now().UnixNano()
		// Set the Redis key (UUID) to the timestamp value.
		err = rdb.Set(ctx, key.String(), value, expr).Err()
		if err != nil {
			util.Warn(err, "rdb.Set failed")
			continue
		}

		// Fetch the timestamp by the UUID key.
		val, err := rdb.Get(ctx, key.String()).Result()
		endSetGet := time.Now().UnixNano()
		// Calculate the elapsed time between setting and retrieving the value in Redis.
		delta := endSetGet - startSetGet
		if err != nil {
			util.Warn(err, "rdb.Get failed")
			continue
		}
		// Load the user from Redis
		var loaded User
		json.Unmarshal([]byte(val), &loaded)

		duration := time.Duration(delta)

		// Record the operation duration using a Prometheus histogram (if enabled).
		if m != nil {
			m.Hist.WithLabelValues("redis").Observe(duration.Seconds())
		}

		// Track latency for p90 calculation
		latencyTracker.Add(duration)

		// Increment the operation counter.
		count++
		totalOps.Add(1)

		// Pause execution to prevent overloading the target.
		if pause > 0 {
			util.Sleep(pause)
		}
	}
}

func main() {
	// Define flags.
	addr := flag.String("addr", "", "Redis address")
	password := flag.String("password", "", "Redis password")
	initialRate := flag.Int("initial-rate", 1000, "Initial number of requests per second per client")
	rateIncrease := flag.Int("rate-increase", 1000, "Rate increase per second (ops/sec per second)")
	pause := flag.Int("pause", 0, "Delays between requests in microseconds")
	poolSize := flag.Int("pool-size", 1, "Number of connections per Redis client")
	duration := flag.Int("duration", 1200, "Test duration in seconds")
	tickInterval := flag.Float64("tick-interval", 1.0, "Interval in seconds between spawning new clients and reporting (0.1, 1, or 10)")
	enableMetrics := flag.Bool("enable-metrics", false, "Enable Prometheus metrics on port 8082")
	cpuprofile := flag.String("cpuprofile", "", "Write CPU profile to file")

	// Parse the flags.
	flag.Parse()

	// Start CPU profiling if requested
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("Could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("Could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		fmt.Printf("# CPU profiling enabled, writing to %s\n", *cpuprofile)
	}

	fmt.Printf("# Connecting to %s, initial rate per client: %d, rate increase: %d ops/sec per second, pause: %d\n", *addr, *initialRate, *rateIncrease, *pause)
	fmt.Printf("# Test duration: %d seconds, tick interval: %.1f seconds\n", *duration, *tickInterval)

	// Create Prometheus metrics if enabled.
	var m *mon.Metrics
	if *enableMetrics {
		reg := prometheus.NewRegistry()
		m = mon.NewMetrics("client", []string{"version"}, []string{}, []string{"target"}, reg)
		mon.StartPrometheus(8082, reg)
		fmt.Println("# Prometheus metrics enabled on :8082")
	}

	// Print CSV header
	fmt.Println("elapsed_time_sec,active_clients,expected_rate_ops_sec,actual_ops_sec,total_ops,p90_latency_ms")

	// Create a context with timeout for the test duration.
	testCtx, cancel := context.WithTimeout(context.Background(), time.Duration(*duration)*time.Second)
	defer cancel()

	// WaitGroup to track all worker goroutines.
	var wg sync.WaitGroup

	// Atomic counter to track total operations across all clients.
	var totalOps atomic.Int64

	// Latency tracker for p90 calculation.
	latencyTracker := &LatencyTracker{
		latencies: make([]time.Duration, 0, 10000),
	}

	// Track the overall start time.
	overallStart := time.Now()

	// Ticker to spawn a new client at the specified interval.
	tickDuration := time.Duration(*tickInterval * float64(time.Second))
	ticker := time.NewTicker(tickDuration)
	defer ticker.Stop()

	clientID := 0

	// Spawn the first client immediately.
	clientID++
	wg.Add(1)
	go worker(testCtx, clientID, *addr, *password, *initialRate, *pause, *poolSize, m, &wg, &totalOps, latencyTracker)
	fmt.Printf("# Client %d started with target rate: %d ops/sec, pool size: %d\n", clientID, *initialRate, *poolSize)

	// Monitor and spawn new clients at the specified tick interval.
	go func() {
		lastOps := int64(0)
		lastTime := overallStart
		for {
			select {
			case <-testCtx.Done():
				// Test duration reached, stop spawning new clients
				return
			case <-ticker.C:
				now := time.Now()
				secondsElapsed := now.Sub(overallStart).Seconds()
				intervalDuration := now.Sub(lastTime).Seconds()
				lastTime = now

				currentTotalOps := totalOps.Load()
				opsThisInterval := currentTotalOps - lastOps
				lastOps = currentTotalOps

				// Calculate ops/sec based on actual interval duration
				opsPerSec := int64(float64(opsThisInterval) / intervalDuration)

				// Get p90 latency and reset for next interval
				p90Latency := latencyTracker.GetAndResetP90()

				// Spawn a new client.
				clientID++
				wg.Add(1)
				go worker(testCtx, clientID, *addr, *password, *initialRate, *pause, *poolSize, m, &wg, &totalOps, latencyTracker)

				expectedRate := *initialRate + (int(secondsElapsed) * *rateIncrease)
				// Print CSV format: elapsed_time,active_clients,expected_rate_ops_sec,actual_ops_sec,total_ops,p90_latency_ms
				fmt.Printf("%.1f,%d,%d,%d,%d,%.2f\n",
					secondsElapsed, clientID, expectedRate, opsPerSec, currentTotalOps, p90Latency)
			}
		}
	}()

	// Wait for all workers to complete.
	wg.Wait()

	// Print final summary
	fmt.Printf("# Test completed after %d seconds\n", *duration)
	fmt.Printf("# Total operations: %d\n", totalOps.Load())
	fmt.Printf("# Average throughput: %.2f ops/sec\n", float64(totalOps.Load())/float64(*duration))
}
