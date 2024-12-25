.PHONY: test cover generate

build:
	go build -v ./...

test:
	# find . -name "go.mod" -execdir go test ./... \;
	go test ./... -v -race
	go test ./sns/... -v race

cover:
	$(eval PACKAGES := $(shell go list ./... | grep -v -e internal && go list ./sns/...))

	go test $(PACKAGES) -v -coverprofile=coverage.out
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	open -a "Google Chrome" coverage.html

generate:
	go generate ./...

lint:
	#golines -w .
	golangci-lint run -v

push-check:
	gofmt -w .
	goimports -w .
	go mod tidy
	make build
	make lint