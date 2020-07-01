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
RUN echo $RANDOM > skipcache

COPY ./src ./src
RUN cargo build

#################
# testing image #
#################
FROM joeygibson/cucumber-tester:v1.1.3 as test

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y unixodbc-dev curl

RUN curl -L https://www.vertica.com/client_drivers/10.0.x/10.0.0-0/vertica-client-10.0.0-0.x86_64.tar.gz \
    | tar -C / -xzf -

ENV PATH="/opt/vertica/bin:${PATH}"

COPY --from=builder /tmp/verticaextractor/target/debug/verticaextractor .