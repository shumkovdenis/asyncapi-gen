FROM golang:1.23-alpine

ENV GO111MODULE=on \
    CGO_ENABLED=0

WORKDIR /go/src/app

COPY go.mod go.sum ./

RUN go mod download && \
    go get github.com/githubnemo/CompileDaemon && \
    go install github.com/githubnemo/CompileDaemon

WORKDIR /go/src/app/example

EXPOSE 3000

ENTRYPOINT CompileDaemon -build="go build -o /go/bin/app" -command="/go/bin/app"