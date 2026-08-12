FROM golang:1.26.5

ENV GOTOOLCHAIN=auto

WORKDIR /app

CMD ["go", "test", "-v", "-tags=integration,local", "./test/integration/replication/..."]
