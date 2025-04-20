FROM golang:1.23.5 AS builder
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/output_gateway ./gateway/cmd/output_gateway
RUN chmod +x bin/output_gateway

FROM busybox:latest
COPY --from=builder /build/bin/output_gateway /output_gateway
COPY ./gateway/config/output_config.yaml /output_gateway_config.yaml

ENTRYPOINT ["/output_gateway"]