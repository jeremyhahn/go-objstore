# Dockerfile for TypeScript SDK testing
FROM node:22-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    python3 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package files for dependency caching
COPY sdks/typescript/package*.json ./
COPY sdks/typescript/tsconfig.json ./
COPY sdks/typescript/tsconfig.cjs.json ./
COPY sdks/typescript/tsconfig.esm.json ./

# Install dependencies
RUN npm ci

# Copy proto files (required for gRPC client)
# Proto files are mounted at runtime via docker-compose volumes
# COPY proto ./proto

# Copy build helper scripts and source code
COPY sdks/typescript/scripts ./scripts
COPY sdks/typescript/src ./src
COPY sdks/typescript/tests ./tests

# Build TypeScript
RUN npm run build

# Default command runs unit tests
CMD ["npm", "run", "test:unit"]
