FROM rust:1.87-alpine as builder
ARG TARGET
WORKDIR /src
COPY . .
RUN cargo build --example $TARGET

FROM alpine
ARG TARGET
COPY --from=builder /src/target/debug/examples/${TARGET} /usr/local/bin/example
ENTRYPOINT ["/usr/local/bin/example"]
