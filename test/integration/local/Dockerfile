FROM golang:1.23

ENV GOTOOLCHAIN=auto

WORKDIR /app

CMD ["go", "test", "-v", "-tags=integration,local", "./test/integration/local/..."]
