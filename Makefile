.PHONY: run fmt tidy

run:
	go run ./cmd/ordersvc

fmt:
	go fmt ./...

tidy:
	go mod tidy