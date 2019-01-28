FROM ubuntu:18.04

RUN apt update
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    ca-certificates curl file git \
    build-essential pkg-config libssl-dev \
    autoconf automake autotools-dev libtool xutils-dev

RUN useradd -ms /bin/bash rust

USER rust
ENV HOME /home/rust
ENV USER rust
ENV SHELL /bin/bash
WORKDIR /home/rust

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain stable -y

RUN echo "export PATH=~/.cargo/bin:$PATH" >> ~/.bashrc
RUN /bin/bash -c "source .cargo/env"
ENV RUST_LOG mieql=info

COPY Cargo.toml .
COPY Cargo.lock .
COPY src src

RUN /bin/bash -c "./.cargo/bin/cargo build --release"

ENTRYPOINT ["./target/release/mieql"]