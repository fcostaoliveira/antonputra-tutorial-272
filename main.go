package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
func worker(clientID int, addr, password string, targetRate int, pause int, m *mon.Metrics, wg *sync.WaitGroup, totalOps *atomic.Int64, latencyTracker *LatencyTracker) {
	defer wg.Done()

	// Create Redis client for this worker.
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer rdb.Close()

	// Set a 1-second TTL on the Redis keys.
	expr := time.Duration(time.Duration(1) * time.Second)

	// Initialize the start time and the counter to track the number of operations.
	var start time.Time = time.Now()
	var count int = 0

	// Start an infinite loop and perform the work.
	for {
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

		// Set the Redis key (UUID) to the timestamp value.
		err = rdb.Set(ctx, key.String(), value, expr).Err()
		if err != nil {
			util.Warn(err, "rdb.Set failed")
			continue
		}

		// Fetch the timestamp by the UUID key.
		val, err := rdb.Get(ctx, key.String()).Result()
		if err != nil {
			util.Warn(err, "rdb.Get failed")
			continue
		}

		// Load the user from Redis
		var loaded User
		json.Unmarshal([]byte(val), &loaded)

		// Calculate the elapsed time between setting and retrieving the value in Redis.
		delta := time.Now().UnixNano() - loaded.Timestamp
		duration := time.Duration(delta)

		// Record the operation duration using a Prometheus histogram.
		m.Hist.WithLabelValues("redis").Observe(duration.Seconds())

		// Track latency for p90 calculation
		latencyTracker.Add(duration)

		// Increment the operation counter.
		count++
		totalOps.Add(1)

		// Pause execution to prevent overloading the target.
		util.Sleep(pause)
	}
}

func main() {
	// Define flags.
	addr := flag.String("addr", "", "Redis address")
	password := flag.String("password", "", "Redis password")
	initialRate := flag.Int("initial-rate", 1000, "Initial number of requests per second per client")
	rateIncrease := flag.Int("rate-increase", 1000, "Rate increase per second (ops/sec per second)")
	pause := flag.Int("pause", 100, "Delays between requests in microseconds")

	// Parse the flags.
	flag.Parse()
	fmt.Printf("# Connecting to %s, initial rate per client: %d, rate increase: %d ops/sec per second, pause: %d\n", *addr, *initialRate, *rateIncrease, *pause)

	// Create Prometheus metrics.
	reg := prometheus.NewRegistry()
	m := mon.NewMetrics("client", []string{"version"}, []string{}, []string{"target"}, reg)
	mon.StartPrometheus(8082, reg)

	// Print CSV header
	fmt.Println("second,active_clients,expected_rate_ops_sec,actual_ops_sec,total_ops,p90_latency_ms")

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

	// Ticker to spawn a new client every second.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	clientID := 0

	// Spawn the first client immediately.
	clientID++
	wg.Add(1)
	go worker(clientID, *addr, *password, *initialRate, *pause, m, &wg, &totalOps, latencyTracker)
	fmt.Printf("# Client %d started with target rate: %d ops/sec\n", clientID, *initialRate)

	// Monitor and spawn new clients every second.
	go func() {
		lastOps := int64(0)
		for range ticker.C {
			secondsElapsed := int(time.Since(overallStart).Seconds())
			currentTotalOps := totalOps.Load()
			opsThisSecond := currentTotalOps - lastOps
			lastOps = currentTotalOps

			// Get p90 latency and reset for next interval
			p90Latency := latencyTracker.GetAndResetP90()

			// Spawn a new client.
			clientID++
			wg.Add(1)
			go worker(clientID, *addr, *password, *initialRate, *pause, m, &wg, &totalOps, latencyTracker)

			expectedRate := *initialRate + (secondsElapsed * *rateIncrease)
			// Print CSV format: second,active_clients,expected_rate_ops_sec,actual_ops_sec,total_ops,p90_latency_ms
			fmt.Printf("%d,%d,%d,%d,%d,%.2f\n",
				secondsElapsed, clientID, expectedRate, opsThisSecond, currentTotalOps, p90Latency)
		}
	}()

	// Wait for all workers (this will run indefinitely).
	wg.Wait()
}
