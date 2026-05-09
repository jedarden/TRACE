//! Log file writer with buffered writes and rotation support
//!
//! Manages hourly log file rotation with buffered writes for performance.
//! On rotation, flushes buffer, closes file, and signals Flusher by renaming
//! the file with a .ready extension.

use anyhow::Result;
use chrono::Utc;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use tracing::{debug, info, warn};

/// Current hour key for file rotation (UTC)
fn current_hour_key() -> String {
    Utc::now().format("%Y%m%d-%H").to_string()
}

/// Get log file path for a given hour key
fn log_file_path(log_dir: &PathBuf, hour_key: &str) -> PathBuf {
    log_dir.join(format!("raw-{}.jsonl", hour_key))
}

/// Get ready file path (signals Flusher to process)
fn ready_file_path(log_dir: &PathBuf, hour_key: &str) -> PathBuf {
    log_dir.join(format!("raw-{}.jsonl.ready", hour_key))
}

/// Log file writer with buffered writes and rotation support
pub struct LogFileWriter {
    /// Base directory for log files
    log_dir: PathBuf,
    /// Current hour bucket
    current_hour: String,
    /// Buffered writer for current log file
    writer: Option<BufWriter<File>>,
}

impl LogFileWriter {
    /// Create a new log file writer
    pub fn new(log_dir: PathBuf) -> Result<Self> {
        let hour_key = current_hour_key();
        let writer = Self::open_log_file(&log_dir, &hour_key)?;

        info!("Log file writer initialized for hour {}", hour_key);

        Ok(Self {
            log_dir,
            current_hour: hour_key,
            writer: Some(writer),
        })
    }

    /// Open a new log file for writing
    fn open_log_file(log_dir: &PathBuf, hour_key: &str) -> Result<BufWriter<File>> {
        let path = log_file_path(log_dir, hour_key);

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open file in append mode, create if it doesn't exist
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        debug!("Opened log file: {}", path.display());

        // Use BufWriter with 64KB buffer for better performance
        Ok(BufWriter::with_capacity(64 * 1024, file))
    }

    /// Write a JSON line to the current log file
    pub fn write_line(&mut self, json_line: &str) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writeln!(writer, "{}", json_line)?;
            Ok(())
        } else {
            anyhow::bail!("Log writer not initialized");
        }
    }

    /// Check for and perform rotation if hour has changed
    /// Returns true if rotation was performed
    pub fn check_rotation(&mut self) -> Result<bool> {
        let new_hour = current_hour_key();

        if self.current_hour != new_hour {
            self.rotate(&new_hour)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Perform log rotation:
    /// 1. Flush current buffer
    /// 2. Close current file
    /// 3. Rename to .ready to signal Flusher
    /// 4. Open new file for new hour
    fn rotate(&mut self, new_hour: &str) -> Result<()> {
        info!(
            "Rotating log files: {} -> {}",
            self.current_hour, new_hour
        );

        // Flush and close current writer
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            let file = writer.into_inner()?;
            file.sync_all()?;
            drop(file);
        }

        // Rename completed file to signal Flusher
        let old_path = log_file_path(&self.log_dir, &self.current_hour);
        let ready_path = ready_file_path(&self.log_dir, &self.current_hour);

        if old_path.exists() {
            std::fs::rename(&old_path, &ready_path)?;
            info!(
                "Signaled Flusher: {} -> {}",
                old_path.display(),
                ready_path.display()
            );
        }

        // Open new file for new hour
        self.current_hour = new_hour.to_string();
        self.writer = Some(Self::open_log_file(&self.log_dir, new_hour)?);

        info!("Log rotation complete, now writing to hour {}", new_hour);

        Ok(())
    }

    /// Get the current hour key
    pub fn current_hour(&self) -> &str {
        &self.current_hour
    }

    /// Flush the buffer (useful for testing and immediate writes)
    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
            Ok(())
        } else {
            anyhow::bail!("Log writer not initialized");
        }
    }

    /// Prepare for shutdown by flushing buffer and closing file
    /// Also renames the current file to .ready to signal Flusher
    pub fn prepare_shutdown(&mut self) -> Result<()> {
        info!("Preparing shutdown: flushing buffer and closing file");

        // Flush and close current writer
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            let file = writer.into_inner()?;
            file.sync_all()?;
            drop(file);
        }

        // Rename current file to signal Flusher
        let current_path = log_file_path(&self.log_dir, &self.current_hour);
        let ready_path = ready_file_path(&self.log_dir, &self.current_hour);

        if current_path.exists() {
            std::fs::rename(&current_path, &ready_path)?;
            info!(
                "Shutdown complete, signaled Flusher: {}",
                ready_path.display()
            );
        } else {
            debug!("No current log file to signal on shutdown");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_log_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let writer = LogFileWriter::new(log_dir.clone()).unwrap();

        assert_eq!(writer.current_hour(), current_hour_key());
    }

    #[test]
    fn test_write_line() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let mut writer = LogFileWriter::new(log_dir.clone()).unwrap();

        writer.write_line(r#"{"test": "data"}"#).unwrap();
        writer.flush().unwrap();

        let path = log_file_path(&log_dir, writer.current_hour());
        let content = fs::read_to_string(path).unwrap();

        assert!(content.contains(r#"{"test": "data"}"#));
    }

    #[test]
    fn test_rotation_creates_ready_file() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();

        let mut writer = LogFileWriter::new(log_dir.clone()).unwrap();

        // Write some data
        writer.write_line(r#"{"test": "before"}"#).unwrap();

        // Force rotation by directly calling rotate with a new hour
        let new_hour = format!("{}-00", &writer.current_hour()[..8]);
        writer.rotate(&new_hour).unwrap();

        // Check that a .ready file exists
        let ready_path = ready_file_path(&log_dir, &writer.current_hour());
        assert!(ready_path.exists());
    }
}
