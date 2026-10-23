FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Pass build metadata (no .git in the image): docker build --build-arg VERSION=$(git describe --tags --always --dirty) --build-arg COMMIT=$(git rev-parse --short HEAD)
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build -ldflags="-s -w     -X github.com/DojoGenesis/policy-data-infrastructure/internal/version.Version=${VERSION}     -X github.com/DojoGenesis/policy-data-infrastructure/internal/version.Commit=${COMMIT}"     -o /pdi ./cmd/pdi

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /pdi /usr/local/bin/pdi
ENTRYPOINT ["pdi"]
CMD ["serve", "--port", "8340"]
