FROM golang:1.23.5 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/input_gateway ./gateway/cmd/input_gateway
RUN chmod +x bin/input_gateway

FROM busybox:latest
COPY --from=builder /build/bin/input_gateway /input_gateway
COPY ./gateway/config/input_config.yaml /input_gateway_config.yaml

ENTRYPOINT ["/input_gateway"]