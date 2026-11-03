FROM golang:1.22-alpine@sha256:1699c10032ca2582ec89a24a1312d986a3f094aed3d5c1147b19880afe40e052 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/logister-api ./cmd/api && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/logister-kafka-load ./cmd/kafka-load

FROM scratch
WORKDIR /app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/logister-api /app/logister-api
COPY --from=build /out/logister-kafka-load /app/logister-kafka-load
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/logister-api"]
