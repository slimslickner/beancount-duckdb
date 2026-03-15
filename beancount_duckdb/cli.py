"""CLI entry point for beancount-duckdb."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from beancount_duckdb.loader import load


def resolve_db_path(explicit: Path | None) -> Path | None:
    """Resolve the DuckDB database path.

    Precedence (lowest → highest):
      1. None (in-memory, print usage hint)
      2. ``BEANCOUNT_DB`` environment variable
      3. Explicit CLI argument
    """
    if explicit is not None:
        return explicit
    env = os.environ.get("BEANCOUNT_DB")
    if env:
        return Path(env)
    return None


def _cmd_load(args: argparse.Namespace) -> None:
    db_path = resolve_db_path(args.db_file)
    conn = load(
        args.beancount_file,
        path=db_path,
        post_sql_files=args.post_sql or [],
        tags_yaml=args.tags_yaml,
    )
    conn.close()
    if db_path is None:
        print(
            "No output path specified. To use in Python or Marimo:\n"
            "\n"
            "    import beancount_duckdb\n"
            "    conn = beancount_duckdb.load('your.bean')\n"
            "\n"
            "Or write a persistent file:\n"
            "\n"
            f"    beancount-duckdb load {args.beancount_file} ledger.duckdb\n"
        )
    else:
        print(f"Written to {db_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="beancount-duckdb",
        description="Load a Beancount ledger into a DuckDB database.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    load_parser = subparsers.add_parser(
        "load",
        help="Parse a Beancount file and write it to DuckDB.",
    )
    load_parser.add_argument(
        "beancount_file",
        type=Path,
        help="Path to the .bean ledger file.",
    )
    load_parser.add_argument(
        "db_file",
        type=Path,
        nargs="?",
        default=None,
        help=(
            "Path for the output .duckdb file. "
            "Defaults to BEANCOUNT_DB env var. "
            "If unset, prints a usage hint for Python/Marimo."
        ),
    )
    load_parser.add_argument(
        "--post-sql",
        type=Path,
        metavar="FILE",
        action="append",
        default=None,
        help=(
            "SQL file to execute after the main load. "
            "Can be repeated to run multiple files in order."
        ),
    )
    load_parser.add_argument(
        "--tags-yaml",
        type=Path,
        metavar="FILE",
        default=None,
        help="YAML file mapping tag names to label/group metadata.",
    )
    load_parser.set_defaults(func=_cmd_load)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    args.func(args)


if __name__ == "__main__":
    main()
