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

fix-lint:
	gofmt -w .
	goimports -w .
	gofumpt -w .
	find . -name '*.go' | xargs gci write --skip-generated -s standard -s default
	go mod tidy


lint:
	make fix-lint
	golangci-lint run -v

push-check:
	make lint
	make test