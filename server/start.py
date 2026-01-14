#!/usr/bin/env python
"""Startup script for uvicorn with custom configuration."""
import os
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))

    print(f"Starting server on port {port} with 100MB body limit...")

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        timeout_keep_alive=30,
        limit_concurrency=10,
        limit_max_requests=100,
        # Critical: Set max request body size to 100MB
        # This prevents "connection closed" errors on large uploads
        timeout_graceful_shutdown=10,
    )
