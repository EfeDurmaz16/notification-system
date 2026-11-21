# syntax=docker/dockerfile:1
FROM golang:1.26.11-alpine AS deps
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

FROM deps AS build
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/notify ./cmd/provider

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/ ./
USER 65532:65532
# Compose supplies separate probes for the API and simulator. Putting the
# worker probe on the image keeps older worker images usable during rollout.
HEALTHCHECK --interval=10s --timeout=4s --retries=3 CMD wget -q -O /dev/null "${HEALTH_URL:-http://127.0.0.1:8082/readyz}"
ENTRYPOINT ["/app/notify"]
