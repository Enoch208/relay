FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/relay ./cmd/relay && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/relay-worker ./cmd/relay-worker

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && addgroup -S relay && adduser -S -G relay relay
COPY --from=build /out/relay /out/relay-worker /usr/local/bin/
USER relay
EXPOSE 8080
ENTRYPOINT ["relay"]
