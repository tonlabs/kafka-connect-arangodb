FROM maven:3.6-jdk-8 as build
WORKDIR /home/app
COPY src /home/app/src
COPY checkstyle.xml /home/app
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean compile assembly:single

FROM confluentinc/cp-kafka-connect:5.3.1


RUN wget https://github.com/Landoop/kafka-connect-tools/releases/download/v1.0.6/connect-cli -O /usr/bin/connect-cli && \
    chmod +x /usr/bin/connect-cli
RUN apt-get update && apt-get install -y --force-yes bsdmainutils jq

RUN rm -rf /usr/share/java/kafka-connect-arangodb/*
COPY --from=build /home/app/target/* /usr/share/java/kafka-connect-arangodb/

