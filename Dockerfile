# syntax=docker/dockerfile:1
# Builds both services. Select one with --target api | worker.

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.21 AS api
RUN adduser -D -H -u 10001 runbox
COPY --from=build /out/runbox-api /out/runbox-admin /usr/local/bin/
USER runbox
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/readyz || exit 1
ENTRYPOINT ["runbox-api"]

# The worker talks to the Docker socket, which is root-equivalent on the
# host. It runs as root inside its container because socket access requires
# it; see docs/THREAT_MODEL.md for how to deploy it safely.
FROM alpine:3.21 AS worker
COPY --from=build /out/runbox-worker /out/runbox-admin /usr/local/bin/
EXPOSE 9091
ENTRYPOINT ["runbox-worker"]
