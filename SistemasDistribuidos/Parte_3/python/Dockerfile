FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files
COPY server.py client.py proxy.py reference.py ./

# Default environment (overridden per-service in docker-compose)
ENV MODE=server \
    BIND_ADDR=tcp://*:5555 \
    DATA_DIR=/data \
    SERVER_NAME=python-server \
    BOT_NAME=python-bot \
    SERVER_ADDRS=tcp://localhost:5555 \
    PROXY_XSUB=tcp://pubsub-proxy:5557 \
    PROXY_XPUB=tcp://pubsub-proxy:5558 \
    REF_ADDR=tcp://reference:5559 \
    REF_TIMEOUT=5000 \
    HEARTBEAT_EVERY=10 \
    STARTUP_DELAY=8.0 \
    MAX_RETRIES=8 \
    RETRY_DELAY=2.0 \
    RECV_TIMEOUT=10000

RUN mkdir -p /data

# MODE=server    → python server.py
# MODE=client    → python client.py
# MODE=proxy     → python proxy.py
# MODE=reference → python reference.py
CMD ["sh", "-c", "\
  if   [ \"$MODE\" = \"client\" ];    then python client.py; \
  elif [ \"$MODE\" = \"proxy\" ];     then python proxy.py; \
  elif [ \"$MODE\" = \"reference\" ]; then python reference.py; \
  else python server.py; \
  fi"]
