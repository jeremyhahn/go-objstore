# Dockerfile for Python SDK unit testing
# Context: This directory (api/sdks/python)
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements files first for better caching
COPY setup.py README.md ./
COPY objstore ./objstore

# Install Python dependencies
RUN pip install --no-cache-dir -e ".[dev]" && \
    pip install --no-cache-dir h2 httpx[http2,http3]

# Copy test files
COPY tests ./tests

# Default command runs unit tests
CMD ["pytest", "tests/unit/", "-v", "--cov=objstore", "--cov-report=term-missing"]
