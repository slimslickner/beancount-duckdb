"""Tests for beancount_duckdb loader and CLI using example.beancount."""

from __future__ import annotations

import datetime
import os
import subprocess
import sys
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

import beancount_duckdb
from beancount_duckdb.cli import resolve_db_path

EXAMPLE = Path(__file__).parent.parent / "example.beancount"

# Known counts from example.beancount — used to catch silent data loss.
EXPECTED_TRANSACTIONS = 1146
EXPECTED_POSTINGS = 3549
EXPECTED_ACCOUNTS = 60
EXPECTED_PRICES = 930
EXPECTED_SCHEMA_DESCRIPTIONS = 14  # one per built-in view


@pytest.fixture(scope="module")
def conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """In-memory connection loaded from example.beancount, shared across tests."""
    c = beancount_duckdb.load(EXAMPLE)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# loader.load()
# ---------------------------------------------------------------------------

class TestLoad:
    def test_returns_connection(self, conn):
        assert isinstance(conn, duckdb.DuckDBPyConnection)

    def test_all_tables_exist(self, conn):
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        }
        expected = {
            "account", "account_category", "account_currency",
            "transaction", "posting", "tag", "link",
            "transaction_tag", "transaction_link",
            "assertion", "price", "commodity",
            "document", "note", "event", "query", "custom",
            "transaction_metadata", "posting_metadata",
            "open_metadata", "close_metadata", "commodity_metadata",
            "balance_metadata", "note_metadata", "document_metadata",
            "price_metadata", "tag_metadata",
            "schema_description",
        }
        assert expected.issubset(tables)

    def test_all_views_exist(self, conn):
        views = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'main' AND table_type = 'VIEW'"
            ).fetchall()
        }
        expected = {
            "v_accounts", "v_commodities", "v_tags", "v_transactions",
            "v_postings", "v_spending", "v_income", "v_prices",
            "v_assertions", "v_documents", "v_notes", "v_events",
            "v_queries", "v_custom",
        }
        assert expected.issubset(views)

    def test_transaction_count(self, conn):
        count = conn.execute('SELECT COUNT(*) FROM "transaction"').fetchone()[0]
        assert count == EXPECTED_TRANSACTIONS

    def test_posting_count(self, conn):
        count = conn.execute("SELECT COUNT(*) FROM posting").fetchone()[0]
        assert count == EXPECTED_POSTINGS

    def test_account_count(self, conn):
        count = conn.execute("SELECT COUNT(*) FROM account").fetchone()[0]
        assert count == EXPECTED_ACCOUNTS

    def test_first_transaction_values(self, conn):
        row = conn.execute(
            'SELECT date, payee, narration FROM "transaction" ORDER BY id LIMIT 1'
        ).fetchone()
        assert row[0] == datetime.date(2013, 1, 1)
        assert row[1] == ""
        assert "Opening Balance" in row[2]

    def test_first_posting_values(self, conn):
        row = conn.execute(
            "SELECT date, amount_number, amount_currency FROM posting ORDER BY id LIMIT 1"
        ).fetchone()
        assert row[0] == datetime.date(2013, 1, 1)
        assert row[1] == Decimal("3219.170000000")
        assert row[2] == "USD"


# ---------------------------------------------------------------------------
# Native types
# ---------------------------------------------------------------------------

class TestNativeTypes:
    def test_date_is_date(self, conn):
        row = conn.execute("SELECT date FROM v_transactions LIMIT 1").fetchone()
        assert isinstance(row[0], datetime.date)

    def test_amount_number_is_decimal(self, conn):
        row = conn.execute("SELECT amount_number FROM v_postings LIMIT 1").fetchone()
        assert isinstance(row[0], Decimal)

    def test_posting_amount_column_type(self, conn):
        col = conn.execute(
            "SELECT data_type FROM information_schema.columns"
            " WHERE table_name = 'posting' AND column_name = 'amount_number'"
        ).fetchone()
        assert col[0] == "DECIMAL(19,9)"

    def test_transaction_date_column_type(self, conn):
        col = conn.execute(
            "SELECT data_type FROM information_schema.columns"
            " WHERE table_name = 'transaction' AND column_name = 'date'"
        ).fetchone()
        assert col[0] == "DATE"

    def test_cost_date_is_date_or_null(self, conn):
        # cost_date column should be DATE, not VARCHAR
        col = conn.execute(
            "SELECT data_type FROM information_schema.columns"
            " WHERE table_name = 'posting' AND column_name = 'cost_date'"
        ).fetchone()
        assert col[0] == "DATE"


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

class TestViews:
    def test_v_spending_only_expenses(self, conn):
        bad = conn.execute(
            "SELECT COUNT(*) FROM v_spending WHERE account_type != 'Expenses'"
        ).fetchone()[0]
        assert bad == 0

    def test_v_income_only_income(self, conn):
        bad = conn.execute(
            "SELECT COUNT(*) FROM v_income WHERE account_type != 'Income'"
        ).fetchone()[0]
        assert bad == 0

    def test_v_transactions_tags_is_string_or_null(self, conn):
        # Every non-null tags value must be a plain string (no list/array leakage).
        row = conn.execute(
            "SELECT tags FROM v_transactions WHERE tags IS NOT NULL LIMIT 1"
        ).fetchone()
        assert row is not None
        assert isinstance(row[0], str)

    def test_v_transactions_tagged_count(self, conn):
        # example.beancount has a known number of tagged transactions.
        count = conn.execute(
            "SELECT COUNT(*) FROM v_transactions WHERE tags IS NOT NULL"
        ).fetchone()[0]
        assert count == 92

    def test_v_postings_no_orphaned_accounts(self, conn):
        # Every posting must join to a valid account — no NULLs from a broken join.
        nulls = conn.execute(
            "SELECT COUNT(*) FROM v_postings WHERE account IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_v_postings_account_types_valid(self, conn):
        bad = conn.execute(
            "SELECT DISTINCT account_type FROM v_postings"
            " WHERE account_type NOT IN ('Assets','Liabilities','Equity','Income','Expenses')"
        ).fetchall()
        assert bad == []

    def test_v_prices_count(self, conn):
        count = conn.execute("SELECT COUNT(*) FROM v_prices").fetchone()[0]
        assert count == EXPECTED_PRICES

    def test_v_prices_join_does_not_drop_rows(self, conn):
        # v_prices uses LEFT JOIN on commodity — row count must equal raw price table.
        raw = conn.execute("SELECT COUNT(*) FROM price").fetchone()[0]
        view = conn.execute("SELECT COUNT(*) FROM v_prices").fetchone()[0]
        assert view == raw

    def test_schema_description_all_views_seeded(self, conn):
        count = conn.execute(
            "SELECT COUNT(*) FROM schema_description WHERE object_type = 'view'"
        ).fetchone()[0]
        assert count == EXPECTED_SCHEMA_DESCRIPTIONS


# ---------------------------------------------------------------------------
# Persistent file output
# ---------------------------------------------------------------------------

class TestPersistentFile:
    def test_writes_duckdb_file(self, tmp_path):
        out = tmp_path / "ledger.duckdb"
        conn = beancount_duckdb.load(EXAMPLE, path=out)
        conn.close()
        assert out.exists()
        assert out.stat().st_size > 0

    def test_persistent_file_preserves_row_counts(self, tmp_path):
        out = tmp_path / "ledger.duckdb"
        conn = beancount_duckdb.load(EXAMPLE, path=out)
        conn.close()
        conn2 = duckdb.connect(str(out))
        row = conn2.execute('SELECT COUNT(*) FROM "transaction"').fetchone()
        conn2.close()
        assert row is not None
        assert row[0] == EXPECTED_TRANSACTIONS

    def test_no_tmp_file_left_on_success(self, tmp_path):
        out = tmp_path / "ledger.duckdb"
        conn = beancount_duckdb.load(EXAMPLE, path=out)
        conn.close()
        assert not (tmp_path / "ledger.duckdb.tmp").exists()


# ---------------------------------------------------------------------------
# post_sql_files
# ---------------------------------------------------------------------------

class TestPostSql:
    def test_custom_view_is_queryable(self, tmp_path):
        sql_file = tmp_path / "custom.sql"
        sql_file.write_text(
            "CREATE VIEW v_test AS SELECT COUNT(*) AS n FROM v_transactions;"
        )
        conn = beancount_duckdb.load(EXAMPLE, post_sql_files=[sql_file])
        row = conn.execute("SELECT n FROM v_test").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == EXPECTED_TRANSACTIONS

    def test_custom_view_persisted_in_file(self, tmp_path):
        sql_file = tmp_path / "custom.sql"
        sql_file.write_text("CREATE VIEW v_custom_test AS SELECT 42 AS answer;")
        out = tmp_path / "ledger.duckdb"
        conn = beancount_duckdb.load(EXAMPLE, path=out, post_sql_files=[sql_file])
        conn.close()
        conn2 = duckdb.connect(str(out))
        row = conn2.execute("SELECT answer FROM v_custom_test").fetchone()
        conn2.close()
        assert row is not None
        assert row[0] == 42


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

class TestCLI:
    def test_load_writes_file(self, tmp_path):
        out = tmp_path / "ledger.duckdb"
        result = subprocess.run(
            [sys.executable, "-m", "beancount_duckdb.cli", "load", str(EXAMPLE), str(out)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert out.exists()

    def test_load_prints_written_path(self, tmp_path):
        out = tmp_path / "ledger.duckdb"
        result = subprocess.run(
            [sys.executable, "-m", "beancount_duckdb.cli", "load", str(EXAMPLE), str(out)],
            capture_output=True,
            text=True,
        )
        assert str(out) in result.stdout

    def test_load_no_path_prints_hint(self, tmp_path):
        result = subprocess.run(
            [sys.executable, "-m", "beancount_duckdb.cli", "load", str(EXAMPLE)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )
        assert result.returncode == 0
        assert "beancount_duckdb" in result.stdout

    def test_load_env_var_path(self, tmp_path):
        out = tmp_path / "from_env.duckdb"
        result = subprocess.run(
            [sys.executable, "-m", "beancount_duckdb.cli", "load", str(EXAMPLE)],
            capture_output=True,
            text=True,
            env={**os.environ, "BEANCOUNT_DB": str(out)},
        )
        assert result.returncode == 0
        assert out.exists()


# ---------------------------------------------------------------------------
# resolve_db_path
# ---------------------------------------------------------------------------

class TestResolveDbPath:
    def test_explicit_path_takes_precedence(self, tmp_path):
        p = tmp_path / "explicit.duckdb"
        assert resolve_db_path(p) == p

    def test_env_var_used_when_no_explicit(self, monkeypatch, tmp_path):
        p = tmp_path / "env.duckdb"
        monkeypatch.setenv("BEANCOUNT_DB", str(p))
        assert resolve_db_path(None) == p

    def test_returns_none_with_no_args_no_env(self, monkeypatch):
        monkeypatch.delenv("BEANCOUNT_DB", raising=False)
        assert resolve_db_path(None) is None
