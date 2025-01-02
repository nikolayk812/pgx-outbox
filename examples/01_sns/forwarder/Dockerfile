FROM alpine:3.21

COPY ./build/linux/forwarder /forwarder

RUN chmod +x /forwarder

ENTRYPOINT ["/forwarder"]