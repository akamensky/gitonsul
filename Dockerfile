FROM golang:1.21-alpine3.18 as builder

ADD . /code
# Run tests
RUN go env && cd /code && go test -buildvcs=false ./...
# Compile the binary
RUN go env && cd /code && go build -buildvcs=false -o /gitonsul .

FROM alpine:3.18

LABEL org.opencontainers.image.source=https://github.com/akamensky/gitonsul

COPY --from=builder /gitonsul /bin/gitonsul

ENTRYPOINT ["gitonsul"]
