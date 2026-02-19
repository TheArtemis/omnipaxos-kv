FROM rust:latest AS chef

# Stop if a command fails
RUN set -eux

# Only fetch crates.io index for used crates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# cargo-chef will be cached from the second build onwards
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
# Include correctness-check feature for proper dependency resolution
RUN cargo chef cook --release --recipe-path recipe.json --features correctness-check

# Build application
COPY . .
# Build with correctness-check feature enabled
RUN cargo build --release --bin client --features correctness-check

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/client /usr/local/bin
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/client"]
