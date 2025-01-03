.PHONY: build test cover cover-ci generate fix-lint lint push-check

MODULES := $(shell find . -name "go.mod" -exec dirname {} \;)
PACKAGES := $(shell for module in $(MODULES); do go list $$module/...; done)
COVER_PACKAGES := $(shell echo $(PACKAGES) | tr ' ' '\n' | grep -v -e internal -e examples)

RYUK := TESTCONTAINERS_RYUK_DISABLED=true

build:
	go build -v ./...

docker-build:
	$(MAKE) -C ./examples/01_sns docker-build

test: docker-build
	$(RYUK) go test $(PACKAGES) -v -race \;

cover: cover-ci
	go tool cover -html=coverage.out -o coverage.html
	open -a "Google Chrome" coverage.html

cover-ci:
	$(RYUK) go test $(COVER_PACKAGES) -v -coverprofile=coverage.out
	go tool cover -func=coverage.out

generate:
	go generate ./...

fix-lint:
	gofmt -w .
	goimports -w .
	gofumpt -w .
	find . -name '*.go' | xargs gci write --skip-generated -s standard -s default
	go mod tidy


lint: generate fix-lint
	golangci-lint run -v

push-check: lint test
