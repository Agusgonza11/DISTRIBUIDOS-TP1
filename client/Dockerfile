FROM golang:1.23.5 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client ./cmd
RUN chmod +x bin/client 

FROM busybox:latest
COPY --from=builder /build/bin/client /client
COPY ./config/config.yaml /config.yaml
RUN mkdir -p /app/data

ENTRYPOINT ["/client"]