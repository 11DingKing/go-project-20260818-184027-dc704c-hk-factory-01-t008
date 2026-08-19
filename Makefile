.PHONY: build test vet fmt run-gateway run-mock docker-amd64 docker-arm64

build:
	go build ./...

test:
	go test -timeout=300s -count=1 ./...

race-test:
	go test -race -timeout=420s -count=1 ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

run-gateway:
	go run ./cmd/gateway

run-mock:
	go run ./cmd/upstream-mock

docker-amd64:
	./build_eval_docker.sh regdispatch linux/amd64

docker-arm64:
	./build_eval_docker.sh regdispatch linux/arm64
