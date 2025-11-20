# CPU Profiling Guide

## Running with CPU Profiling

To enable CPU profiling, use the `-cpuprofile` flag:

```bash
./client -addr localhost:6379 -cpuprofile=cpu.prof
```

The profiling data will be written to `cpu.prof` when the program exits (Ctrl+C).

## Analyzing the Profile

### Interactive Mode

```bash
go tool pprof cpu.prof
```

Once in the interactive mode, useful commands:
- `top` - Show top functions by CPU time
- `top10` - Show top 10 functions
- `list <function>` - Show source code for a function
- `web` - Generate a graph (requires graphviz)
- `pdf` - Generate a PDF report (requires graphviz)
- `help` - Show all commands

### Generate Reports

```bash
# Text report of top functions
go tool pprof -text cpu.prof

# Generate a call graph (requires graphviz: sudo apt install graphviz)
go tool pprof -pdf cpu.prof > cpu_profile.pdf

# Generate an SVG graph
go tool pprof -svg cpu.prof > cpu_profile.svg

# Web interface (opens in browser)
go tool pprof -http=:8080 cpu.prof
```

### Example: Top CPU Consumers

```bash
# Show top 20 functions by CPU time
go tool pprof -top cpu.prof

# Show top functions with cumulative time
go tool pprof -cum -top cpu.prof
```

## Common Analysis Patterns

### Find Bottlenecks
```bash
go tool pprof -top20 cpu.prof | grep -v runtime
```

### Focus on Your Code
```bash
go tool pprof -focus=main cpu.prof
```

### Compare Two Profiles
```bash
go tool pprof -base=cpu1.prof cpu2.prof
```

## Tips

1. **Run for sufficient time**: Profile for at least 30-60 seconds to get meaningful data
2. **Use Ctrl+C to stop**: This ensures the profile is properly written
3. **Web interface is best**: Use `-http=:8080` for the most interactive experience
4. **Install graphviz**: For visual graphs: `sudo apt install graphviz`

## Example Workflow

```bash
# 1. Run the client with profiling for 60 seconds
./client -addr localhost:6379 -cpuprofile=cpu.prof &
CLIENT_PID=$!

# 2. Wait 60 seconds
sleep 60

# 3. Stop the client
kill -INT $CLIENT_PID

# 4. Analyze with web interface
go tool pprof -http=:8080 cpu.prof
```

