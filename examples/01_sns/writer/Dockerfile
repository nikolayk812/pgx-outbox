FROM alpine:3.21

COPY ./build/linux/writer /writer

RUN chmod +x /writer

ENTRYPOINT ["/writer"]