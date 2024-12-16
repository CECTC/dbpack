FROM golang:1.23.3 as builder
WORKDIR /app
ADD . /app
RUN make build-local

FROM alpine:latest
WORKDIR /
COPY --from=builder /app/dbpack /dbpack
CMD ["/dbpack", "start", "-c", "config.yaml"]
