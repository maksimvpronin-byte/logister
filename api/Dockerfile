FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod ./
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /out/logister-api ./cmd/api

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=build /out/logister-api /app/logister-api
EXPOSE 8080
ENTRYPOINT ["/app/logister-api"]
