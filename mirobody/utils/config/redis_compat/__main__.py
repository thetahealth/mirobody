"""Entry point: python -m mirobody.utils.config.redis_compat [--host 0.0.0.0] [--port 6389]"""

import argparse
import asyncio

from .server import RedisCompatServer


def main():
    parser = argparse.ArgumentParser(description="RedisCompat server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6389)
    args = parser.parse_args()

    server = RedisCompatServer(host=args.host, port=args.port)
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nshutting down.")


if __name__ == "__main__":
    main()
