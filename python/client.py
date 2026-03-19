"""
BBS/IRC-like Messaging System - Python Bot Client
Distributed Systems Project - Part 1

Automated bot that: logs in, lists channels, creates a channel.
Uses: ZeroMQ REQ socket + MessagePack serialization
"""

import os
import time
import sys
import zmq
import msgpack

# ── Configuration ─────────────────────────────────────────────────────────────
# Comma-separated list of server addresses, e.g. "tcp://python-server-1:5555,tcp://java-server-1:5555"
SERVER_ADDRS = os.environ.get("SERVER_ADDRS", "tcp://localhost:5555").split(",")
BOT_NAME     = os.environ.get("BOT_NAME", "python-bot")
MAX_RETRIES  = int(os.environ.get("MAX_RETRIES", "5"))
RETRY_DELAY  = float(os.environ.get("RETRY_DELAY", "2.0"))
RECV_TIMEOUT = int(os.environ.get("RECV_TIMEOUT", "5000"))   # ms


# ── Message helpers ───────────────────────────────────────────────────────────
def pack(msg: dict) -> bytes:
    return msgpack.packb(msg, use_bin_type=True)


def unpack(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)


def now() -> float:
    return time.time()


def fmt_time(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ── Logging ───────────────────────────────────────────────────────────────────
SEPARATOR = "─" * 60

def log_send(bot: str, server: str, msg: dict):
    print(f"\n{SEPARATOR}")
    print(f"[{bot}] → SEND  target={server}  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR)


def log_recv(bot: str, server: str, msg: dict):
    print(f"[{bot}] ← RECV  from={server}  type={msg.get('type')}  ts={fmt_time(msg.get('timestamp', 0))}")
    for k, v in msg.items():
        if k not in ("type", "timestamp"):
            print(f"             {k}={v}")
    print(SEPARATOR, flush=True)


# ── Bot logic for a single server connection ──────────────────────────────────
def run_bot_for_server(server_addr: str, bot_name: str, ctx: zmq.Context):
    """Run the full bot sequence against one server."""
    print(f"\n{'═' * 60}")
    print(f"  {bot_name} → connecting to {server_addr}")
    print(f"{'═' * 60}\n", flush=True)

    socket = ctx.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT)
    socket.setsockopt(zmq.SNDTIMEO, RECV_TIMEOUT)
    socket.connect(server_addr)

    # ── Step 1: Login ──────────────────────────────────────────────────────────
    logged_in = False
    for attempt in range(1, MAX_RETRIES + 1):
        req = {"type": "LOGIN", "timestamp": now(), "username": bot_name}
        log_send(bot_name, server_addr, req)
        socket.send(pack(req))

        try:
            rep = unpack(socket.recv())
            log_recv(bot_name, server_addr, rep)
        except zmq.Again:
            print(f"[{bot_name}] Timeout waiting for LOGIN reply (attempt {attempt}/{MAX_RETRIES})", flush=True)
            time.sleep(RETRY_DELAY)
            continue

        if rep.get("type") == "LOGIN_OK":
            print(f"[{bot_name}] ✓ Login successful on {server_addr}", flush=True)
            logged_in = True
            break
        else:
            print(f"[{bot_name}] ✗ Login failed: {rep.get('error')} (attempt {attempt}/{MAX_RETRIES})", flush=True)
            time.sleep(RETRY_DELAY)

    if not logged_in:
        print(f"[{bot_name}] Could not log in to {server_addr} after {MAX_RETRIES} attempts. Skipping.", flush=True)
        socket.close()
        return

    # ── Step 2: List Channels ──────────────────────────────────────────────────
    req = {"type": "LIST_CHANNELS", "timestamp": now()}
    log_send(bot_name, server_addr, req)
    socket.send(pack(req))

    try:
        rep = unpack(socket.recv())
        log_recv(bot_name, server_addr, rep)
        existing_channels = rep.get("channels", [])
        print(f"[{bot_name}] Channels available on {server_addr}: {existing_channels}", flush=True)
    except zmq.Again:
        print(f"[{bot_name}] Timeout waiting for LIST_CHANNELS reply.", flush=True)
        socket.close()
        return

    # ── Step 3: Create a channel ──────────────────────────────────────────────
    channel_name = f"canal-{bot_name}"
    req = {"type": "CREATE_CHANNEL", "timestamp": now(), "channel_name": channel_name}
    log_send(bot_name, server_addr, req)
    socket.send(pack(req))

    try:
        rep = unpack(socket.recv())
        log_recv(bot_name, server_addr, rep)
        status = rep.get("type")
        if status == "CHANNEL_CREATED":
            print(f"[{bot_name}] ✓ Channel '{channel_name}' created on {server_addr}", flush=True)
        elif status == "CHANNEL_EXISTS":
            print(f"[{bot_name}] ℹ Channel '{channel_name}' already exists on {server_addr}", flush=True)
        else:
            print(f"[{bot_name}] ✗ Channel creation failed: {rep.get('error')}", flush=True)
    except zmq.Again:
        print(f"[{bot_name}] Timeout waiting for CREATE_CHANNEL reply.", flush=True)

    socket.close()
    print(f"\n[{bot_name}] Done with {server_addr}\n", flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    # Give servers a moment to start up
    startup_delay = float(os.environ.get("STARTUP_DELAY", "3.0"))
    print(f"[{BOT_NAME}] Waiting {startup_delay}s for servers to be ready...", flush=True)
    time.sleep(startup_delay)

    ctx = zmq.Context()
    for addr in SERVER_ADDRS:
        addr = addr.strip()
        if addr:
            run_bot_for_server(addr, BOT_NAME, ctx)

    ctx.term()
    print(f"\n[{BOT_NAME}] All interactions complete. Exiting.", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
