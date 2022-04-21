FROM alpine:latest

WORKDIR app

COPY dist/ .

CMD ["./dbpack", "start", "-c", "config.yaml"]