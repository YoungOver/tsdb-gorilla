FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tsdb ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tsdb /tsdb
VOLUME /data
EXPOSE 8428 8089/udp
ENTRYPOINT ["/tsdb", "-addr", ":8428", "-udp", ":8089", "-wal", "/data/wal"]
