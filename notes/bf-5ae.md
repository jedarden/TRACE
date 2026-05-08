# Bead bf-5ae: Flusher Batch Accumulator and Flush Trigger

## Status: Already Implemented

The batch accumulator and flush trigger (size + time) functionality was already implemented in the TRACE flusher as part of the original implementation (commit 886983d) and enhanced with comprehensive tests in commit 393157b.

## Implementation Summary

### Core Components

1. **BatchConfig** (`flusher/src/main.rs:126-139`)
   - `max_batch_size_bytes`: Configurable size limit (default: 10MB)
   - `max_batch_age_secs`: Configurable time limit (default: 300s)
   - Environment variables: `TRACE_BATCH_MAX_SIZE_BYTES`, `TRACE_BATCH_MAX_AGE_SECS`

2. **BatchEntry** (`flusher/src/main.rs:141-147`)
   - Contains: data (Vec<u8>), key (String), added_at (Instant), source_file (PathBuf)

3. **BatchAccumulator** (`flusher/src/main.rs:149-225`)
   - `add()`: Adds entry to batch, returns flush trigger status
   - `should_flush()`: Checks size and time triggers
   - `drain()`: Returns all entries and resets state
   - `size_bytes()`: Current batch size
   - `entry_count()`: Number of entries

4. **AddedToBatch enum** (`flusher/src/main.rs:227-231`)
   - `Continue`: Batch not full
   - `ShouldFlushSize(usize)`: Size limit reached
   - `ShouldFlushTime(u64)`: Time limit reached

### Integration Points

- **File processing** (`process_file()`): Adds converted Parquet data to batch
- **Size trigger**: Flushes immediately when batch size limit reached
- **Time trigger**: Periodic check every 30 seconds triggers flush if age limit exceeded
- **Shutdown**: Flushes remaining entries before graceful shutdown

### Test Coverage

Commit 393157b added comprehensive tests:
- `test_batch_accumulator_size_trigger`: Verifies size-based flushing
- `test_batch_accumulator_time_trigger`: Verifies time-based flushing
- `test_batch_accumulator_drain`: Verifies drain functionality
- `test_batch_accumulator_multiple_partitions`: Verifies per-partition tracking

## Configuration

The batch behavior can be configured via environment variables:
- `TRACE_BATCH_MAX_SIZE_BYTES`: Maximum batch size before flush (default: 10485760)
- `TRACE_BATCH_MAX_AGE_SECS`: Maximum batch age before flush (default: 300)

## Verification

To verify the implementation:
```bash
cd flusher
cargo test  # Runs all batch accumulator tests
```
