FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/scheduler ./cmd/scheduler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/scheduler /scheduler
ENV LISTEN_ADDR=:8080 \
    SHUTDOWN_TIMEOUT=3s
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/scheduler"]