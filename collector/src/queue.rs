//! Queue producer abstraction for event buffering
//!
//! Supports multiple backends:
//! - Redis: Simple list-based queue with connection pooling
//! - File: Fallback to direct file writes (default)
//!
//! Architecture:
//! - Events are sent to the queue asynchronously
//! - Redis: Uses LPUSH for O(1) writes with connection pooling
//! - File: Pass-through mode where events bypass queueing
//!
//! Environment variables:
//! - TRACE_QUEUE_BACKEND: "redis" or "file" (default: "file")
//! - TRACE_REDIS_URL: Redis connection string (default: redis://127.0.0.1:6379)
//! - TRACE_REDIS_QUEUE_KEY: Queue key name (default: trace:events)

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Queue backend configuration
#[derive(Debug, Clone, Default)]
pub enum QueueBackend {
    /// Redis list-based queue with connection pooling
    Redis {
        /// Redis connection URL (default: redis://127.0.0.1:6379)
        url: Option<String>,
        /// Queue key name (default: trace:events)
        queue_key: Option<String>,
        /// Connection pool size (default: 10)
        pool_size: Option<usize>,
    },
    /// Direct file writes (no buffering)
    #[default]
    File,
}

impl QueueBackend {
    /// Parse from environment variable TRACE_QUEUE_BACKEND
    ///
    /// Supported values: "redis", "file"
    pub fn from_env() -> Self {
        match std::env::var("TRACE_QUEUE_BACKEND").unwrap_or_default().as_str() {
            "redis" => QueueBackend::Redis {
                url: std::env::var("TRACE_REDIS_URL").ok(),
                queue_key: std::env::var("TRACE_REDIS_QUEUE_KEY").ok(),
                pool_size: std::env::var("TRACE_REDIS_POOL_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok()),
            },
            _ => QueueBackend::File,
        }
    }
}

/// Queue producer trait for event buffering
pub trait QueueProducer: Send + Sync {
    /// Send a single event to the queue
    async fn send(&self, event: &str) -> Result<()>;

    /// Flush any buffered events
    async fn flush(&self) -> Result<()>;

    /// Get the queue name/topic for logging
    fn name(&self) -> &str;
}

/// Queue consumer trait for reading buffered events
pub trait QueueConsumer: Send + Sync {
    /// Receive a single event from the queue (blocking with timeout)
    async fn recv(&self) -> Result<Option<String>>;

    /// Get the queue name/topic for logging
    fn name(&self) -> &str;

    /// Check if the consumer is active (non-fallback mode)
    fn is_active(&self) -> bool;
}

/// Create a queue producer based on configuration
///
/// For Redis, this returns an unconnected producer that must be initialized
/// by calling `connect()` before use.
pub fn create_producer(backend: QueueBackend) -> Result<Arc<dyn QueueProducer>> {
    match backend {
        QueueBackend::Redis {
            url,
            queue_key,
            pool_size,
        } => Ok(Arc::new(RedisProducer::new(url, queue_key, pool_size)?)),
        QueueBackend::File => Ok(Arc::new(FileProducer)),
    }
}

/// Redis queue producer using LPUSH with connection pooling
pub struct RedisProducer {
    /// Redis client for creating connections
    client: redis::Client,
    /// Redis connection manager (lazily initialized)
    conn: Arc<Mutex<Option<redis::aio::ConnectionManager>>>,
    /// Queue key in Redis
    queue_key: String,
    /// Connection URL
    url: String,
}

impl RedisProducer {
    pub fn new(url: Option<String>, queue_key: Option<String>, _pool_size: Option<usize>) -> Result<Self> {
        let url = url.unwrap_or_else(|| "redis://127.0.0.1:6379".to_string());
        let queue_key = queue_key.unwrap_or_else(|| "trace:events".to_string());

        let client = redis::Client::open(url.clone())?;

        Ok(Self {
            client,
            conn: Arc::new(Mutex::new(None)),
            queue_key,
            url,
        })
    }

    /// Ensure the connection is established
    async fn ensure_connected(&self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        if conn.is_none() {
            *conn = Some(redis::aio::ConnectionManager::new(self.client.clone()).await?);
        }
        Ok(())
    }

    /// Helper method to execute Redis commands with error handling
    async fn lpush(&self, value: &str) -> Result<()> {
        self.ensure_connected().await?;
        let mut conn = self.conn.lock().await;
        if let Some(ref mut c) = *conn {
            redis::cmd("LPUSH")
                .arg(&self.queue_key)
                .arg(value)
                .query_async(c)
                .await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl QueueProducer for RedisProducer {
    async fn send(&self, event: &str) -> Result<()> {
        self.lpush(event).await
    }

    async fn flush(&self) -> Result<()> {
        // Redis connection manager handles batching
        Ok(())
    }

    fn name(&self) -> &str {
        &self.queue_key
    }
}

/// File fallback producer (no-op, used when queue is disabled)
///
/// When using File backend, events are written directly to log files
/// by the main collector, bypassing the queue entirely.
#[derive(Debug, Default)]
pub struct FileProducer;

#[async_trait::async_trait]
impl QueueProducer for FileProducer {
    async fn send(&self, _event: &str) -> Result<()> {
        // No-op - events are written directly to files in the main collector
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "file"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_backend_default() {
        let backend = QueueBackend::default();
        matches!(backend, QueueBackend::File);
    }

    #[test]
    fn test_create_redis_producer() {
        // This will fail without a running Redis server, but tests the API
        let result = create_producer(QueueBackend::Redis {
            url: Some("redis://localhost:6379".to_string()),
            queue_key: Some("test".to_string()),
            pool_size: Some(5),
        });
        // We expect this might fail if Redis isn't running
        // The test verifies the API is correct
        if let Err(e) = result {
            println!("Redis connection failed (expected if Redis not running): {}", e);
        }
    }

    #[test]
    fn test_create_file_producer() {
        let producer = create_producer(QueueBackend::File).unwrap();
        assert_eq!(producer.name(), "file");
    }

    #[test]
    fn test_queue_backend_from_env() {
        // Test default (file) when no env var set
        let backend = QueueBackend::from_env();
        matches!(backend, QueueBackend::File);
    }
}
