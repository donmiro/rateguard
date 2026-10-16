# rateguard-sidecar, from the repository root:
#
#     docker build -f rateguard-sidecar/Dockerfile -t rateguard-sidecar .
#
# A static-enough binary on distroless: no shell, no package manager, a
# non-root user.

FROM rust:1.88-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p rateguard-sidecar

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/rateguard-sidecar /usr/local/bin/rateguard-sidecar
# Gossip between instances, and the API on loopback; 9090 for health and
# metrics when --metrics-listen is set.
EXPOSE 7946/udp 9090/tcp
ENTRYPOINT ["/usr/local/bin/rateguard-sidecar"]
