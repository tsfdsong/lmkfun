FROM golang:1.23 as builder

RUN apt-get update && apt-get install -y \
    ca-certificates \
    bash \
    gcc \
    g++ \
    curl \
    make \
    openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work

# Copy everything from the current directory to the PWD inside the container
COPY . .

RUN go mod download

RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o solsdk ./cmd

######## Start a new stage from scratch #######
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    bash \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /root/

COPY --from=builder /work/solsdk ./

EXPOSE 8000

CMD ["./solsdk"]