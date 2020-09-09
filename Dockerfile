FROM maven:3.6-jdk-8 as build
WORKDIR /home/app
COPY src /home/app/src
COPY checkstyle.xml /home/app
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package

FROM confluentinc/cp-kafka-connect:5.3.1


RUN wget https://github.com/Landoop/kafka-connect-tools/releases/download/v1.0.6/connect-cli -O /usr/bin/connect-cli && \
    chmod +x /usr/bin/connect-cli
RUN apt-get update && apt-get install -y --force-yes bsdmainutils jq

COPY --from=build /home/app/target/kafka-connect-arangodb-1.0.7.jar /usr/share/java/kafka-connect-arangodb/

