# Mini MapReduce

A simplified MapReduce implementation in Java

## Objective

Build a distributed MapReduce system consisting of a **Coordinator** and multiple **Workers** that
communicate via RPC.
The Coordinator hands out Map and Reduce tasks to Workers, handles fault tolerance (reassigning
tasks after 10-second
timeouts), and the system produces the same output as a sequential single-process run.

## Architecture

```
                        ┌──────────┐
               ┌───────>│ Worker 1 │──────┐
               │        └──────────┘      │
┌─────────────┐│        ┌──────────┐      │   ┌─────────────┐
│ Input Files │├───────>│ Worker 2 │──────├──>│ Output Files │
└─────────────┘│  RPC   └──────────┘  RPC │   │  mr-out-*   │
               │        ┌──────────┐      │   └─────────────┘
               └───────>│ Worker N │──────┘
                        └──────────┘
                   ▲                  │
                   └──── Coordinator ─┘
                        (task mgmt)
```

### Phase 1 - Map

1. Coordinator assigns each input file as a Map task to a Worker.
2. Worker reads the file, calls `MapFunc.map(filename, contents)` producing `List<KeyValue>`.
3. Worker partitions output into `nReduce` buckets using `hash(key) % nReduce`.
4. Intermediate results written to files named `mr-X-Y` (X = map task #, Y = reduce bucket #).

### Phase 2 - Reduce

1. Coordinator assigns Reduce tasks once all Map tasks complete.
2. Worker reads all `mr-*-Y` files for its assigned bucket Y.
3. Sorts by key, groups values per key, calls `ReduceFunc.reduce(key, values)`.
4. Output written to `mr-out-Y` with format: `key value` per line.

### Fault Tolerance

- Coordinator tracks task assignment timestamps.
- If a Worker doesn't complete a task within 10 seconds, the task is reassigned.
- Workers use atomic file writes (write to temp file, then rename) to prevent partial output.

## Progress

- [x] Core data types (`KeyValue`, `MapFunc`, `ReduceFunc` interfaces)
- [x] `WordCount` application (map and reduce)
- [x] Sequential MapReduce runner (`MrSequential`)
- [x] Unit tests for sequential runner
- [x] Coordinator (task state management, RPC server)
- [x] Worker (RPC client, task execution loop)
- [x] RPC message definitions
- [x] Intermediate file I/O (JSON serialization of `KeyValue` pairs)
- [x] Fault tolerance (10s timeout, task reassignment)
- [x] Coordinator `Done()` method (exit condition)
- [x] Integration tests with multiple workers
