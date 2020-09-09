#!/bin/sh
set -e

DOCKER_BUILDKIT=1 docker build --ssh default . -t tonlabs/kafka-connect-arangodb-integration-test:latest
docker push tonlabs/kafka-connect-arangodb-integration-test:latest
