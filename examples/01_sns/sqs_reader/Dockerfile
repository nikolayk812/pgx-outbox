FROM alpine:3.23.3

COPY ./build/linux/sqs_reader /sqs_reader

RUN chmod +x /sqs_reader

ENTRYPOINT ["/sqs_reader"]