FROM alpine:3.21

COPY ./build/linux/sqs_reader /sqs_reader

RUN chmod +x /sqs_reader

ENTRYPOINT ["/sqs_reader"]