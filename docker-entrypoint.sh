#!/bin/bash
set -e

# Signal handler for graceful shutdown
shutdown() {
    echo "Shutting down..."
    if [ -n "$COLLECTOR_PID" ]; then
        kill -TERM "$COLLECTOR_PID" 2>/dev/null || true
    fi
    if [ -n "$FLUSHER_PID" ]; then
        kill -TERM "$FLUSHER_PID" 2>/dev/null || true
    fi
    wait
    echo "Shutdown complete"
}

trap shutdown SIGTERM SIGINT

# Start collector in background
echo "Starting collector..."
/app/trace-collector &
COLLECTOR_PID=$!

# Start flusher in background
echo "Starting flusher..."
/app/trace-flusher &
FLUSHER_PID=$!

# Wait for any process to exit
wait -n

# If either process exits, shut down the other
EXIT_CODE=$?
echo "Process exited with code $EXIT_CODE"
shutdown
exit $EXIT_CODE
