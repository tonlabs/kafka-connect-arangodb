FROM maven:3.6-jdk-8 as build
WORKDIR /home/app
COPY src /home/app/src
COPY checkstyle.xml /home/app
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean compile assembly:single

FROM confluentinc/cp-kafka-connect:6.1.1

USER root
WORKDIR /home/root

RUN wget https://github.com/Landoop/kafka-connect-tools/releases/download/v1.0.6/connect-cli -O /usr/bin/connect-cli && \
    chmod +x /usr/bin/connect-cli
RUN yum install -y jq

RUN rm -rf /usr/share/java/kafka-connect-arangodb/*
COPY --from=build /home/app/target/* /usr/share/java/kafka-connect-arangodb/

COPY plugins /usr/share/java

RUN confluent-hub install --no-prompt confluentinc/connect-transforms:latest
