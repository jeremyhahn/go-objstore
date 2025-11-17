FROM golang:1.23

RUN apt-get update && apt-get install -y ca-certificates curl netcat-openbsd && rm -rf /var/lib/apt/lists/*

ENV GOTOOLCHAIN=auto

WORKDIR /app

# Wait for MinIO to be ready before running tests
CMD ["sh", "-c", "echo 'Waiting for MinIO to be ready...'; for i in 1 2 3 4 5 6 7 8 9 10; do curl -sf http://minio:9000/minio/health/live > /dev/null && break || sleep 2; done; echo 'MinIO is ready, running tests...'; go test -v -tags=integration,minio ./test/integration/minio/..."]
