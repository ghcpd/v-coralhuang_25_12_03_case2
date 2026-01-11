"""CLI utilities for Outbox Forwarder management."""
from __future__ import annotations
import asyncio
import argparse
import sys
from outbox.forwarder import replay_from_dlq

async def main_replay(args):
    """Replay records from DLQ."""
    await replay_from_dlq(limit=args.limit)

def main():
    parser = argparse.ArgumentParser(description="Outbox Forwarder CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Replay command
    replay_parser = subparsers.add_parser("replay", help="Replay records from DLQ")
    replay_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of records to replay (default: 100)"
    )
    
    args = parser.parse_args()
    
    if args.command == "replay":
        asyncio.run(main_replay(args))
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
