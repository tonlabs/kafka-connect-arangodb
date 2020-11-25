# syntax=docker/dockerfile:experimental
FROM rust:1.45-buster
WORKDIR /send-block
COPY ./send-block/ .
RUN apt-get update -y
RUN apt-get install -y build-essential
RUN apt-get install -y cmake
RUN --mount=type=ssh cargo build
RUN mv ./target/debug/send-block .
RUN apt-get install -y jq
COPY ./kafka-messages/ ./kafka-messages/
COPY kafka-publisher-config.json .
COPY test.sh test.sh
COPY connector-config.json .
RUN chmod +x test.sh
RUN apt-get install -y vim

