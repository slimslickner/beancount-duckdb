"""Core Beancount → DuckDB loader."""

from __future__ import annotations

import datetime
import json
import logging
from decimal import Decimal
from importlib.resources import files
from pathlib import Path
from typing import Any

import duckdb
import yaml

from beancount import loader as bean_loader
from beancount.core import amount as bean_amount
from beancount.core import data

log = logging.getLogger(__name__)

# Keys always stripped from metadata before storing.
_META_SKIP = frozenset({"filename", "lineno"})

# Default descriptions for built-in views, seeded into schema_description.
_BUILTIN_DESCRIPTIONS: list[tuple[str, str, str]] = [
    ("view", "v_accounts", "Accounts with `label` from open_metadata."),
    (
        "view",
        "v_commodities",
        "Commodities with common metadata keys pivoted as columns: `name`, `asset_class`, `asset_subclass`, `quote`.",
    ),
    ("view", "v_tags", "Tags. Join `tag_metadata` for custom attributes loaded via `--tags-yaml`."),
    ("view", "v_transactions", "Transactions with comma-separated `tags` and `links`."),
    ("view", "v_events", "Life events (job changes, moves, etc.)."),
    ("view", "v_queries", "Named BQL queries defined in the ledger."),
    ("view", "v_custom", "Custom directives (Fava config, plugin settings, etc.)."),
    (
        "view",
        "v_postings",
        "All postings joined with account and transaction context. "
        "Includes `account_label` from `v_accounts`.",
    ),
    (
        "view",
        "v_prices",
        "Price entries with commodity display name from `v_commodities`.",
    ),
    ("view", "v_assertions", "Balance assertions with account name and label."),
    ("view", "v_documents", "Document directives with account name and label."),
    ("view", "v_notes", "Note directives with account name and label."),
    (
        "view",
        "v_spending",
        "Expense postings (`account_type = 'Expenses'`). "
        "Filtered subset of `v_postings`.",
    ),
    (
        "view",
        "v_income",
        "Income postings (`account_type = 'Income'`). "
        "Filtered subset of `v_postings`. "
        "Note: `amount_number` is typically negative — use `ABS()` for magnitudes.",
    ),
]


def _encode_meta_value(value: Any) -> tuple[str | None, str] | None:
    """Encode a beancount metadata value as (string_repr, value_type).

    Returns None if the value type is not supported and should be skipped.
    value_type matches the beancount grammar token types:
      str, bool, date, decimal, amount, null
    """
    if value is None:
        return (None, "null")
    if isinstance(value, bool):
        return ("true" if value else "false", "bool")
    if isinstance(value, str):
        return (value, "str")
    if isinstance(value, Decimal):
        return (str(value), "decimal")
    if isinstance(value, datetime.date):
        return (value.isoformat(), "date")
    if isinstance(value, bean_amount.Amount):
        return (f"{value.number} {value.currency}", "amount")
    return None


def _exec_script(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Execute a multi-statement SQL script in DuckDB."""
    for stmt in sql.split(";"):
        # Strip comments and whitespace; skip empty/comment-only chunks.
        lines = [
            line for line in stmt.splitlines() if not line.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if stmt:
            conn.execute(stmt)


def load(
    bean_file: Path | str,
    path: Path | str | None = None,
    post_sql_files: list[Path] | None = None,
    tags_yaml: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Parse *bean_file* and load all directives into a DuckDB database.

    Args:
        bean_file: Path to the .bean ledger file.
        path: Optional path for a persistent .duckdb file. If None, returns
              an in-memory connection.
        post_sql_files: Optional SQL files to execute after the main load.
        tags_yaml: Optional YAML file with tag metadata.

    Returns:
        An open DuckDB connection with all data loaded and views created.
    """
    loader = BeanDuckDBLoader()
    return loader.load(
        Path(bean_file),
        path=Path(path) if path is not None else None,
        post_sql_files=post_sql_files,
        tags_yaml=tags_yaml,
    )


class BeanDuckDBLoader:
    """Loads a Beancount ledger into a DuckDB database."""

    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._account_map: dict[str, int] = {}
        self._category_map: dict[str, int] = {}

    def load(
        self,
        bean_file: Path,
        path: Path | None = None,
        post_sql_files: list[Path] | None = None,
        tags_yaml: Path | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """Parse *bean_file* and write all directives into the DuckDB database.

        Returns an open connection. For in-memory databases the caller owns it.
        For persistent databases the file is written atomically (temp → rename).
        """
        log.info("Parsing %s", bean_file)
        entries, errors, _ = bean_loader.load_file(str(bean_file))
        if errors:
            for err in errors:
                src = err.source or {}
                log.error(
                    "%s:%s: %s",
                    src.get("filename", "?"),
                    src.get("lineno", "?"),
                    err.message,
                )
            raise SystemExit(1)

        if path is not None:
            # Write to a temp file; rename atomically on success.
            tmp_path = path.with_suffix(".duckdb.tmp")
            log.info("Loading into %s", path)
            conn = duckdb.connect(str(tmp_path))
        else:
            log.info("Loading into in-memory DuckDB")
            conn = duckdb.connect()

        self._conn = conn
        self._account_map = {}
        self._category_map = {}
        success = False
        try:
            self._init_schema()
            self._import_accounts(entries)
            self._import_transactions(entries)
            self._import_balances(entries)
            self._import_prices(entries)
            self._import_commodities(entries)
            self._import_documents(entries, bean_file.parent)
            self._import_notes(entries)
            self._import_events(entries)
            self._import_queries(entries)
            self._import_customs(entries)
            if tags_yaml is not None:
                self._import_tags_yaml(tags_yaml)
            self._init_views()
            self._seed_schema_descriptions()
            for sql_file in post_sql_files or []:
                self._exec_post_sql(sql_file)
            success = True
        finally:
            if not success:
                conn.close()
                self._conn = None
                if path is not None:
                    tmp_path.unlink(missing_ok=True)

        if path is not None:
            conn.close()
            self._conn = None
            tmp_path.rename(path)
            log.info("Done. Written to %s", path)
            return duckdb.connect(str(path))

        log.info("Done.")
        return conn

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_schema(self) -> None:
        assert self._conn is not None
        schema = (files("beancount_duckdb") / "schema.sql").read_text()
        _exec_script(self._conn, schema)

    def _init_views(self) -> None:
        assert self._conn is not None
        views = (files("beancount_duckdb") / "views.sql").read_text()
        _exec_script(self._conn, views)

    def _seed_schema_descriptions(self) -> None:
        assert self._conn is not None
        self._conn.executemany(
            "INSERT INTO schema_description"
            " (object_type, name, description) VALUES (?, ?, ?)"
            " ON CONFLICT DO NOTHING",
            _BUILTIN_DESCRIPTIONS,
        )

    def _exec_post_sql(self, sql_file: Path) -> None:
        """Execute a user-provided SQL file."""
        assert self._conn is not None
        log.info("Running post-sql: %s", sql_file)
        sql = sql_file.read_text()
        try:
            _exec_script(self._conn, sql)
        except Exception:
            log.error("post-sql failed: %s", sql_file)
            raise

    def _get_or_insert_tag(self, name: str) -> int:
        """Return the id for a tag, inserting it if it doesn't exist."""
        assert self._conn is not None
        result = self._conn.execute(
            "INSERT INTO tag (name) VALUES (?) ON CONFLICT DO NOTHING RETURNING id",
            [name],
        ).fetchone()
        if result is not None:
            return result[0]
        row = self._conn.execute(
            "SELECT id FROM tag WHERE name = ?", [name]
        ).fetchone()
        assert row is not None
        return row[0]

    def _get_or_insert_link(self, name: str) -> int:
        """Return the id for a link, inserting it if it doesn't exist."""
        assert self._conn is not None
        result = self._conn.execute(
            "INSERT INTO link (name) VALUES (?) ON CONFLICT DO NOTHING RETURNING id",
            [name],
        ).fetchone()
        if result is not None:
            return result[0]
        row = self._conn.execute(
            "SELECT id FROM link WHERE name = ?", [name]
        ).fetchone()
        assert row is not None
        return row[0]

    def _import_tags_yaml(self, tags_yaml: Path) -> None:
        """Load tag metadata from a YAML file into tag_metadata."""
        assert self._conn is not None
        log.info("Loading tags from %s", tags_yaml)
        raw = yaml.safe_load(tags_yaml.read_text())
        tags = raw.get("tags") if isinstance(raw, dict) else None
        if not tags:
            log.warning("No 'tags' key found in %s — skipping", tags_yaml)
            return
        for name, attrs in tags.items():
            tag_id = self._get_or_insert_tag(name)
            if not isinstance(attrs, dict):
                continue
            for key, value in attrs.items():
                if value is None:
                    value_str, value_type = None, "null"
                elif isinstance(value, bool):
                    value_str, value_type = ("true" if value else "false"), "bool"
                else:
                    value_str, value_type = str(value), "str"
                self._conn.execute(
                    'INSERT INTO tag_metadata (tag_id, "key", "value", value_type)'
                    " VALUES (?, ?, ?, ?)"
                    ' ON CONFLICT (tag_id, "key") DO UPDATE SET'
                    ' "value" = excluded."value", value_type = excluded.value_type',
                    [tag_id, key, value_str, value_type],
                )

    def _ensure_category(self, account_type: str, categories: list[str]) -> int:
        """Return the leaf category ID, creating any missing hierarchy nodes."""
        assert self._conn is not None
        parent_id: int | None = None
        for name in categories:
            key = f"{account_type}:{name}:{parent_id}"
            if key in self._category_map:
                parent_id = self._category_map[key]
                continue
            # Select first to handle NULL parent_id correctly.
            row = self._conn.execute(
                "SELECT id FROM account_category"
                " WHERE name = ? AND parent_id IS NOT DISTINCT FROM ? AND account_type = ?",
                [name, parent_id, account_type],
            ).fetchone()
            if row is not None:
                self._category_map[key] = row[0]
                parent_id = row[0]
                continue
            result = self._conn.execute(
                "INSERT INTO account_category (name, parent_id, account_type)"
                " VALUES (?, ?, ?) RETURNING id",
                [name, parent_id, account_type],
            ).fetchone()
            assert result is not None, f"Failed to insert category {name!r}"
            self._category_map[key] = result[0]
            parent_id = result[0]
        assert parent_id is not None
        return parent_id

    def _insert_meta(
        self,
        table: str,
        fk_col: str,
        fk_id: int,
        meta: dict[str, Any],
        skip_keys: frozenset[str] = frozenset(),
    ) -> None:
        """Insert normalized metadata rows for one directive."""
        assert self._conn is not None
        rows = []
        for key, value in meta.items():
            if key in _META_SKIP or key.startswith("__") or key in skip_keys:
                continue
            encoded = _encode_meta_value(value)
            if encoded is None:
                log.debug(
                    "Skipping metadata key %r: unsupported type %s",
                    key,
                    type(value).__name__,
                )
                continue
            value_str, value_type = encoded
            rows.append([fk_id, key, value_str, value_type])
        if rows:
            self._conn.executemany(
                f'INSERT INTO {table} ({fk_col}, "key", "value", value_type)'  # noqa: S608
                " VALUES (?, ?, ?, ?)",
                rows,
            )

    # ------------------------------------------------------------------
    # Directive importers
    # ------------------------------------------------------------------

    def _import_accounts(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if isinstance(entry, data.Open):
                parts = entry.account.split(":")
                account_type = parts[0]
                categories = parts[1:]
                cat_id = self._ensure_category(account_type, categories)
                result = self._conn.execute(
                    "INSERT INTO account"
                    " (name, account_type, account_category_id, open_date)"
                    " VALUES (?, ?, ?, ?) RETURNING id",
                    [entry.account, account_type, cat_id, entry.date],
                ).fetchone()
                assert result is not None
                account_id = result[0]
                self._account_map[entry.account] = account_id
                if entry.currencies:
                    for currency in entry.currencies:
                        self._conn.execute(
                            "INSERT INTO account_currency"
                            " (account_id, currency) VALUES (?, ?)"
                            " ON CONFLICT DO NOTHING",
                            [account_id, currency],
                        )
                self._insert_meta("open_metadata", "account_id", account_id, entry.meta)

            elif isinstance(entry, data.Close):
                self._conn.execute(
                    "UPDATE account SET close_date = ? WHERE name = ?",
                    [entry.date, entry.account],
                )
                account_id = self._account_map.get(entry.account)
                if account_id is not None:
                    self._insert_meta(
                        "close_metadata", "account_id", account_id, entry.meta
                    )

    def _import_transactions(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Transaction):
                continue

            result = self._conn.execute(
                'INSERT INTO "transaction" (date, flag, payee, narration)'
                " VALUES (?, ?, ?, ?) RETURNING id",
                [entry.date, entry.flag, entry.payee or "", entry.narration],
            ).fetchone()
            assert result is not None
            txn_id = result[0]
            self._insert_meta(
                "transaction_metadata", "transaction_id", txn_id, entry.meta
            )

            for tag in entry.tags:
                tag_id = self._get_or_insert_tag(tag)
                self._conn.execute(
                    "INSERT INTO transaction_tag"
                    " (transaction_id, tag_id) VALUES (?, ?)"
                    " ON CONFLICT DO NOTHING",
                    [txn_id, tag_id],
                )

            for link in entry.links:
                link_id = self._get_or_insert_link(link)
                self._conn.execute(
                    "INSERT INTO transaction_link"
                    " (transaction_id, link_id) VALUES (?, ?)"
                    " ON CONFLICT DO NOTHING",
                    [txn_id, link_id],
                )

            for posting in entry.postings:
                account_id = self._account_map.get(posting.account)
                if account_id is None:
                    log.warning(
                        "Unknown account %r in transaction on %s — skipping posting",
                        posting.account,
                        entry.date,
                    )
                    continue

                units = posting.units
                price = posting.price
                cost = posting.cost

                p_result = self._conn.execute(
                    "INSERT INTO posting ("
                    " date, account_id, transaction_id, flag,"
                    " amount_number, amount_currency,"
                    " price_number, price_currency,"
                    " cost_number, cost_currency, cost_date, cost_label"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
                    [
                        entry.date,
                        account_id,
                        txn_id,
                        posting.flag,
                        units.number,
                        units.currency,
                        price.number if price is not None else None,
                        price.currency if price is not None else None,
                        cost.number
                        if cost is not None and cost.number is not None
                        else None,
                        cost.currency if cost is not None else None,
                        cost.date if cost is not None and cost.date is not None else None,
                        cost.label if cost is not None else None,
                    ],
                ).fetchone()
                assert p_result is not None
                self._insert_meta(
                    "posting_metadata", "posting_id", p_result[0], posting.meta
                )

    def _import_balances(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Balance):
                continue
            account_id = self._account_map.get(entry.account)
            if account_id is None:
                log.warning(
                    "Unknown account %r in balance on %s — skipping",
                    entry.account,
                    entry.date,
                )
                continue
            result = self._conn.execute(
                "INSERT INTO assertion"
                " (date, account_id, amount_number, amount_currency)"
                " VALUES (?, ?, ?, ?) RETURNING id",
                [entry.date, account_id, entry.amount.number, entry.amount.currency],
            ).fetchone()
            assert result is not None
            self._insert_meta(
                "balance_metadata", "assertion_id", result[0], entry.meta
            )

    def _import_prices(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Price):
                continue
            result = self._conn.execute(
                "INSERT INTO price (date, currency, amount_number, amount_currency)"
                " VALUES (?, ?, ?, ?) RETURNING id",
                [entry.date, entry.currency, entry.amount.number, entry.amount.currency],
            ).fetchone()
            assert result is not None
            self._insert_meta("price_metadata", "price_id", result[0], entry.meta)

    def _import_commodities(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Commodity):
                continue
            decimal_places = int(entry.meta.get("decimal_places", 0))
            result = self._conn.execute(
                "INSERT INTO commodity (date, currency, decimal_places) VALUES (?, ?, ?)"
                " ON CONFLICT DO NOTHING RETURNING id",
                [entry.date, entry.currency, decimal_places],
            ).fetchone()
            if result is not None:
                self._insert_meta(
                    "commodity_metadata",
                    "commodity_id",
                    result[0],
                    entry.meta,
                    skip_keys=frozenset({"decimal_places"}),
                )

    def _import_documents(self, entries: list[Any], base_path: Path) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Document):
                continue
            account_id = self._account_map.get(entry.account)
            if account_id is None:
                log.warning(
                    "Unknown account %r in document on %s — skipping",
                    entry.account,
                    entry.date,
                )
                continue
            try:
                filename = str(Path(entry.filename).relative_to(base_path))
            except ValueError:
                filename = entry.filename
            result = self._conn.execute(
                "INSERT INTO document (date, account_id, filename) VALUES (?, ?, ?)"
                " RETURNING id",
                [entry.date, account_id, filename],
            ).fetchone()
            assert result is not None
            self._insert_meta("document_metadata", "document_id", result[0], entry.meta)

    def _import_notes(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Note):
                continue
            account_id = self._account_map.get(entry.account)
            if account_id is None:
                log.warning(
                    "Unknown account %r in note on %s — skipping",
                    entry.account,
                    entry.date,
                )
                continue
            result = self._conn.execute(
                "INSERT INTO note (date, account_id, comment) VALUES (?, ?, ?)"
                " RETURNING id",
                [entry.date, account_id, entry.comment],
            ).fetchone()
            assert result is not None
            self._insert_meta("note_metadata", "note_id", result[0], entry.meta)

    def _import_events(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Event):
                continue
            self._conn.execute(
                "INSERT INTO event (date, type, description) VALUES (?, ?, ?)",
                [entry.date, entry.type, entry.description],
            )

    def _import_queries(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Query):
                continue
            self._conn.execute(
                'INSERT INTO "query" (date, name, query_string) VALUES (?, ?, ?)',
                [entry.date, entry.name, entry.query_string],
            )

    def _import_customs(self, entries: list[Any]) -> None:
        assert self._conn is not None
        for entry in entries:
            if not isinstance(entry, data.Custom):
                continue
            self._conn.execute(
                'INSERT INTO "custom" (date, type, "values") VALUES (?, ?, ?)',
                [
                    entry.date,
                    entry.type,
                    json.dumps(
                        [
                            str(v.value) if hasattr(v, "value") else str(v)
                            for v in entry.values
                        ]
                    ),
                ],
            )
