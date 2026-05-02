# Build stage
FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -q -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=builder /build/target/bbs-messaging-1.0-SNAPSHOT.jar app.jar

ENV MODE=server \
    BIND_ADDR=tcp://*:5555 \
    DATA_DIR=/data \
    SERVER_NAME=java-server \
    BOT_NAME=java-bot \
    SERVER_ADDRS=tcp://localhost:5555 \
    PROXY_XSUB=tcp://pubsub-proxy:5557 \
    PROXY_XPUB=tcp://pubsub-proxy:5558 \
    REF_ADDR=tcp://reference:5559 \
    REF_TIMEOUT=5000 \
    SYNC_EVERY=15 \
    ELECTION_BIND=tcp://*:5560 \
    ELECTION_PORT=5560 \
    STARTUP_DELAY_MS=8000 \
    MAX_RETRIES=8 \
    RETRY_DELAY_MS=2000 \
    RECV_TIMEOUT=10000

RUN mkdir -p /data

CMD ["java", "-jar", "app.jar"]
