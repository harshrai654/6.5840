# MapReduce Implementation

A complete implementation of Google's MapReduce programming model for distributed data processing and parallel computation.

## Overview

MapReduce is a programming model and framework for processing large datasets in parallel across a cluster of machines. It abstracts away the complexities of distributed programming—data distribution, parallelization, and fault tolerance—allowing developers to focus on the core logic of their `Map` and `Reduce` functions.

This implementation provides:

- **Scalability**: Horizontal scaling by adding more worker machines
- **Parallelism**: Inherent parallelization of Map and Reduce tasks
- **Fault Tolerance**: Automatic re-execution of failed tasks
- **Simplicity**: Developers only need to define Map and Reduce functions

## Core Concepts

MapReduce breaks data processing into five key stages:

### 1. **Input Splitting**

The input dataset is divided into `M` smaller, independent chunks (typically 16-64MB). Each chunk will be processed by one Map task.

### 2. **Map Phase**

- A user-defined `Map` function is applied to each input chunk in parallel
- Takes an input pair (key, value) and produces intermediate key/value pairs
- Example: Word count - Map emits `(word, 1)` for each word in the input

### 3. **Shuffle and Sort Phase**

- Intermediate key/value pairs are gathered and sorted by key
- All values for the same key are grouped together
- Data is partitioned into `R` regions (one for each Reduce task) using a partitioning function: `hash(key) % R`

### 4. **Reduce Phase**

- A user-defined `Reduce` function processes grouped data for each unique key
- Takes a key and a list of all values for that key
- Produces final output (typically one or zero output per key)
- Example: Word count Reduce sums all `1`s to get the total count

### 5. **Output**

Final results are written to `R` output files corresponding to each Reduce task partition.

## Execution Flow

```
Input Files
    ↓
[Master/Coordinator] ← orchestrates
    ↓
Map Phase (M tasks)          Shuffle Phase        Reduce Phase (R tasks)
- Read input splits    →     - Sort by key   →    - Aggregate values
- Apply user Map()           - Partition           - Apply user Reduce()
- Partition output           - Intermediate       - Final output
                              files
```

**Key Design Pattern:**

- **Master/Coordinator**: Central authority that assigns tasks and tracks progress
- **Workers**: Execute Map or Reduce tasks independently
- **Intermediate Files**: Local disk storage bridges Map and Reduce phases
- **Atomic File Renaming**: Ensures consistency and crash safety

## Implementation Highlights

### Coordinator

- Manages task assignment and tracking
- Detects worker failures and re-assigns tasks
- Coordinates between Map and Reduce phases
- Determines task completion

### Worker

- Executes assigned Map or Reduce tasks
- **Map Task Processing**:

  - Reads input file
  - Applies user-defined `mapf` function
  - Partitions output using `hash(key) % nReduce`
  - Sorts each partition in-memory
  - Writes to temporary files, atomically renamed

- **Reduce Task Processing**:
  - Gathers all intermediate data for its partition from Map workers
  - Reads and sorts all collected KeyValue pairs
  - Groups by key and applies user-defined `reducef`
  - Writes final output with atomic rename

### Fault Tolerance

- **Map Task Failure**: Input is durable, task is re-executed on a different worker
- **Reduce Task Failure**: Task is re-executed; intermediate files are preserved
- **Worker Crash After Map**: Intermediate files remain; another Reduce can read them
- **Timeout Detection**: Coordinator detects unresponsive workers

### Optimization Techniques

- **In-Memory Sorting**: Each partition's data is sorted efficiently in memory
- **Atomic File Writes**: Temporary files prevent partial reads
- **Worker-Specific Directories**: Isolates intermediate files per worker
- **JSON Encoding**: Efficient serialization of KeyValue pairs

## Typical Usage

```go
// Define your Map function
func mapf(filename string, contents string) []mr.KeyValue {
    // Parse input and emit intermediate key/value pairs
}

// Define your Reduce function
func reducef(key string, values []string) string {
    // Aggregate values and produce output
}

// The framework handles distribution and coordination
```

## Testing

The implementation passes standard MapReduce tests including:

- Word count on large text files
- Basic agreement on task execution
- Failure handling and recovery
- Concurrent task processing
- Output correctness

## References

- **Original MapReduce Paper**: [MapReduce: Simplified Data Processing on Large Clusters](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)
- **Lab Assignment**: [MIT 6.824 - MapReduce Lab](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
- **Blog Post**: [Map Reduce Implementation Walkthrough](https://harshrai654.github.io/blogs/map-reduce/)

## Key Takeaways

- **Abstraction Power**: MapReduce elegantly abstracts distributed complexity
- **Fault Tolerance Design**: Task re-execution is simpler than global recovery
- **Data Locality**: Intermediate files allow Reducers to efficiently fetch data
- **Simplicity for Scale**: Simple Map and Reduce logic can process enormous datasets
- **Practical Distributed Systems**: Understanding failures, timeouts, and recovery is crucial

---

_For detailed implementation walkthrough and design decisions, refer to the [blog post](https://harshrai654.github.io/blogs/map-reduce/)._
