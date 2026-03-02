FROM alpine:3.23.3

COPY ./build/linux/forwarder /forwarder

RUN chmod +x /forwarder

ENTRYPOINT ["/forwarder"]