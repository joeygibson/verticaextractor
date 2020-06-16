FROM rust:1.44 AS builder

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y unixodbc-dev

RUN cd /tmp && USER=root cargo new --bin verticaextractor
WORKDIR /tmp/verticaextractor

# cache rust dependencies in docker layer
COPY Cargo.toml Cargo.lock ./
RUN touch build.rs && echo "fn main() {println!(\"cargo:rerun-if-changed=\\\"/tmp/<projectname>/build.rs\\\"\");}" >> build.rs
RUN cargo build

# build the real stuff and disable cache via the ADD
#ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache

COPY ./src ./src
RUN cargo build

RUN mkdir -p /opt/bin && cp target/debug/verticaextractor /opt/bin/

FROM builder AS test-image

ENTRYPOINT ["/opt/bin/verticaextractor"]
