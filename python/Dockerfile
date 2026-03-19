FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY server.py client.py ./

# Default to server mode; override with MODE=client
ENV MODE=server \
    BIND_ADDR=tcp://*:5555 \
    DATA_DIR=/data \
    SERVER_NAME=python-server \
    BOT_NAME=python-bot \
    SERVER_ADDRS=tcp://localhost:5555 \
    STARTUP_DELAY=3.0 \
    MAX_RETRIES=5 \
    RETRY_DELAY=2.0 \
    RECV_TIMEOUT=5000

# Create data directory
RUN mkdir -p /data

CMD ["sh", "-c", "if [ \"$MODE\" = \"client\" ]; then python client.py; else python server.py; fi"]
