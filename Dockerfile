FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /pdi ./cmd/pdi

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /pdi /usr/local/bin/pdi
ENTRYPOINT ["pdi"]
CMD ["serve", "--port", "8340"]
