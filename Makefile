.PHONY: test cover generate

build:
	go build -v ./...

test:
	go test ./... -v -race

cover:
	$(eval EXCLUDE := fakes mocks containers)
	$(eval PACKAGES := $(shell go list ./... | grep -v $(foreach exclude,$(EXCLUDE), -e $(exclude))))

	go test $(PACKAGES) -v -coverprofile=coverage.out
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	open -a "Google Chrome" coverage.html

generate:
	go generate ./...

lint:
	golines -w .
	golangci-lint run -v

push-check:
	gofmt -w .
	goimports -w .
	make build
	make lint