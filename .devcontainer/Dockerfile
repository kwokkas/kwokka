FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    liburing-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# Stable toolchain pinned to match rust-toolchain.toml (keep the two in
# sync), plus a nightly toolchain carrying miri for `cargo +nightly miri`
# and rustfmt for `cargo +nightly fmt` (rustfmt.toml uses nightly-only
# options, so CI formats on nightly).
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
    -y \
    --default-toolchain 1.95 \
    --profile minimal \
    --component clippy rustfmt rust-src llvm-tools \
    && rustup toolchain install nightly --profile minimal --component miri rust-src rustfmt

# Toolbelt as prebuilt binaries via cargo-binstall, no source compile:
# nextest, cargo-deny, cargo-hack, cargo-llvm-cov, machete, semver-checks,
# taplo, typos. loom is a crate dep, not a CLI, so it needs no install here.
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
    https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash \
    && cargo binstall -y \
        cargo-nextest \
        cargo-deny \
        cargo-hack \
        cargo-llvm-cov \
        cargo-machete \
        cargo-semver-checks \
        taplo-cli \
        typos-cli
