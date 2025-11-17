FROM golang:1.23

RUN apt-get update && apt-get install -y ca-certificates netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

ENV GOTOOLCHAIN=auto

WORKDIR /app

# Wait for Azurite to be ready before running tests
CMD ["sh", "-c", "echo 'Waiting for Azurite to be ready...'; for i in 1 2 3 4 5 6 7 8 9 10; do nc -z azurite 10000 && break || sleep 2; done; echo 'Azurite is ready, running tests...'; go test -v -tags=integration,azureblob ./test/integration/azure/..."]
