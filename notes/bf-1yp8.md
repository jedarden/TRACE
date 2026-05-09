# Log File Rotation and Buffer Management (bf-1yp8)

## Summary
Implemented hourly log file rotation with buffered writes for the TRACE collector.

## Implementation Details

### Files Modified
- `collector/src/log_writer.rs` (new) - Core log file writer with rotation support
- `collector/src/main.rs` - Integrated log_writer into collector
- `collector/Cargo.toml` - Added tempfile dev dependency for tests

### Key Features

1. **Hourly Rotation**
   - Files are rotated based on UTC hour (format: `raw-YYYYMMDD-HH.jsonl`)
   - Rotation check runs every 60 seconds in background task
   - Uses `current_hour_key()` to track current hour bucket

2. **Buffered Writes**
   - `BufWriter` with 64KB buffer for better performance
   - File remains open for writes within each hour
   - Automatic flush on rotation and shutdown

3. **Rotation Signaling**
   - On rotation, file is renamed to `.ready` extension
   - Signals Flusher process to pick up completed files
   - Example: `raw-20260508-14.jsonl` → `raw-20260508-14.jsonl.ready`

4. **Graceful Shutdown**
   - `prepare_shutdown()` flushes buffer and closes file
   - Current file is renamed to `.ready` before shutdown
   - Integrated with tokio signal handling (Ctrl+C, SIGTERM)

### API

```rust
// Create new log writer
let writer = LogFileWriter::new(log_dir)?;

// Write a JSON line
writer.write_line(&json_line)?;

// Check and perform rotation if needed
if writer.check_rotation()? {
    // Rotation occurred
}

// Graceful shutdown
writer.prepare_shutdown()?;
```

## Tests
Three unit tests included in `log_writer.rs`:
- `test_log_writer_creation` - Verifies initialization
- `test_write_line` - Verifies buffered writes
- `test_rotation_creates_ready_file` - Verifies rotation signaling
