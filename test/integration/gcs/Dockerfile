FROM golang:1.23

RUN apt-get update && apt-get install -y ca-certificates wget netcat-openbsd && rm -rf /var/lib/apt/lists/*

ENV GOTOOLCHAIN=auto

WORKDIR /app

# Wait for fake-gcs to be ready before running tests
CMD ["sh", "-c", "echo 'Waiting for fake-gcs to be ready...'; for i in 1 2 3 4 5 6 7 8 9 10; do wget -q --spider http://fake-gcs:4443/storage/v1/b && break || sleep 2; done; echo 'fake-gcs is ready, running tests...'; go test -v -tags=integration,gcpstorage ./test/integration/gcs/..."]
