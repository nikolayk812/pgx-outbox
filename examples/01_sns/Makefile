.PHONY: image-build docker-build

image-build:
	GOOS=linux GOARCH=amd64 go build -o ./writer/build/linux/writer ./writer
	GOOS=linux GOARCH=amd64 go build -o ./forwarder/build/linux/forwarder ./forwarder
	GOOS=linux GOARCH=amd64 go build -o ./sqs_reader/build/linux/sqs_reader ./sqs_reader

docker-build: image-build
	docker build -t nikolayk812/pgx-outbox-writer -f ./writer/Dockerfile ./writer
	docker build -t nikolayk812/pgx-outbox-forwarder -f ./forwarder/Dockerfile ./forwarder
	docker build -t nikolayk812/pgx-outbox-sqs-reader -f ./sqs_reader/Dockerfile ./sqs_reader
