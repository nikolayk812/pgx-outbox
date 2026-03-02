FROM alpine:3.23.3

COPY ./build/linux/writer /writer

RUN chmod +x /writer

ENTRYPOINT ["/writer"]