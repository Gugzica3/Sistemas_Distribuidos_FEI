"""
BBS/IRC-like Messaging System - Pub/Sub Proxy
Distributed Systems Project - Part 2

ZeroMQ XSUB/XPUB proxy that decouples publishers (servers) from
subscribers (bots). Servers connect to XSUB on :5557 to publish; bots
connect to XPUB on :5558 to subscribe.
"""
import os
import sys
import zmq

XSUB_ADDR = os.environ.get("XSUB_ADDR", "tcp://*:5557")
XPUB_ADDR = os.environ.get("XPUB_ADDR", "tcp://*:5558")


def main():
    ctx = zmq.Context.instance()

    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(XSUB_ADDR)

    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(XPUB_ADDR)

    print("=" * 60, flush=True)
    print(f"  Pub/Sub proxy started", flush=True)
    print(f"  XSUB (publishers ->) : {XSUB_ADDR}", flush=True)
    print(f"  XPUB (-> subscribers): {XPUB_ADDR}", flush=True)
    print("=" * 60, flush=True)

    try:
        zmq.proxy(xsub, xpub)
    except KeyboardInterrupt:
        pass
    finally:
        xsub.close()
        xpub.close()
        ctx.term()
        sys.exit(0)


if __name__ == "__main__":
    main()
