FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml /build/pom.xml
COPY src /build/src

RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre-jammy

WORKDIR /opt/feature-pipeline

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash procps curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/feature-pipeline-1.0.0.jar /opt/feature-pipeline/app.jar
COPY start.sh /opt/feature-pipeline/start.sh

RUN chmod +x /opt/feature-pipeline/start.sh

EXPOSE 8090

CMD ["/opt/feature-pipeline/start.sh"]
