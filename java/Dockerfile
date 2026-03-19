# Build stage
FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /build

# Copy pom first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Copy source and build fat JAR
COPY src ./src
RUN mvn package -q -DskipTests

# ─────────────────────────────────────────────
# Runtime stage (slim JRE)
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=builder /build/target/bbs-messaging-1.0-SNAPSHOT.jar app.jar

# Default environment
ENV MODE=server \
    BIND_ADDR=tcp://*:5555 \
    DATA_DIR=/data \
    SERVER_NAME=java-server \
    BOT_NAME=java-bot \
    SERVER_ADDRS=tcp://localhost:5555 \
    STARTUP_DELAY_MS=3000 \
    MAX_RETRIES=5 \
    RETRY_DELAY_MS=2000 \
    RECV_TIMEOUT=5000

RUN mkdir -p /data

CMD ["java", "-jar", "app.jar"]
