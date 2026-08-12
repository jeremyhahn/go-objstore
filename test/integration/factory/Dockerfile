FROM golang:1.26.5

ENV GOTOOLCHAIN=auto

WORKDIR /app

# The source is baked in rather than bind mounted. A containerised CI runner
# cannot bind mount its own workspace: the host daemon is asked for a path only
# the job container has ("mkdir /workspace: file exists").
COPY . .
RUN go mod download

CMD ["go", "test", "-v", "-tags=integration,local", "./test/integration/factory/..."]
