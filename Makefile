.PHONY: run test bench load docker
run:
	go run ./cmd/server -wal data/wal
test:
	go test -race -count=1 ./...
bench:
	go test -run x -bench . -benchmem ./internal/...
load:
	go run ./cmd/loadgen -hosts 10000 -c 16 -d 20s
docker:
	docker build -t tsdb-gorilla .
