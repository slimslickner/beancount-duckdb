# beancount-duckdb

An opinionated, plugin-ready analytics layer that loads [Beancount](https://beancount.github.io/) ledger data into a queryable DuckDB database.

Beancount is the source of truth. This tool mirrors ledger data into DuckDB so you can query it with standard SQL — CTEs, window functions, aggregations — from Python, [Marimo](https://marimo.io/) notebooks, or any SQL-capable tool.

## Installation

**From a local path:**

```toml
# pyproject.toml
[tool.uv.sources]
beancount-duckdb = { path = "../beancount-duckdb" }

[project]
dependencies = ["beancount-duckdb"]
```

**From GitHub:**

```toml
[tool.uv.sources]
beancount-duckdb = { git = "https://github.com/timtickner/beancount-duckdb" }
```

## Usage

### Python / Marimo

```python
import beancount_duckdb

conn = beancount_duckdb.load("main.bean")
# conn is an open duckdb.DuckDBPyConnection — query it directly
```

In a Marimo notebook with SQL cells, pass the connection as the data source via `mo.sql(..., engine=conn)`.

Custom views from `--post-sql` can be passed at load time and are baked into the returned connection:

```python
conn = beancount_duckdb.load("main.bean", post_sql_files=["custom_views.sql"])
```

**Note:** for in-memory connections, views defined via `post_sql_files` only live for the lifetime of that connection. If you reload, pass `post_sql_files` again. To avoid this, use a persistent file — views are stored in the `.duckdb` file and are available on every open:

```python
# Write once
beancount_duckdb.load("main.bean", path="ledger.duckdb", post_sql_files=["custom_views.sql"])

# Open anywhere, views are already there
import duckdb
conn = duckdb.connect("ledger.duckdb")
```

### CLI

```bash
beancount-duckdb load <beancount_file> [db_file]
```

Without a `db_file`, the ledger is loaded into memory and a usage hint is printed. To write a persistent file:

```bash
beancount-duckdb load main.bean ledger.duckdb
beancount-duckdb load main.bean ~/finance/ledger.duckdb
beancount-duckdb -v load main.bean                       # verbose logging
beancount-duckdb load main.bean ledger.duckdb \
  --tags-yaml tags.yaml \
  --post-sql custom_views.sql
```

The output path can also be set via the `BEANCOUNT_DB` environment variable.

| Flag | Description |
|---|---|
| `--tags-yaml FILE` | Populate tag metadata from a YAML file |
| `--post-sql FILE` | Run a SQL file after the main load; repeatable. Use for custom views or indexes. |

Loading aborts if Beancount reports errors. Persistent file writes are atomic — a temp file is written and renamed on success, so a failed load never corrupts the existing database.

## Query surface

Query using the **views** — they flatten joins and metadata into clean, named columns. Don't query raw tables directly. Additional views can be defined via `--post-sql`.

| View | Description |
|---|---|
| `v_accounts` | Accounts with `label` from open metadata |
| `v_commodities` | Commodities with `name`, `asset_class`, `asset_subclass`, `quote` |
| `v_transactions` | Transactions with comma-separated `tags` and `links` |
| `v_postings` | All postings with account and transaction context |
| `v_spending` | Expense postings — filtered subset of `v_postings` |
| `v_income` | Income postings — filtered subset of `v_postings` |
| `v_prices` | Price entries with commodity display name |
| `v_assertions` | Balance assertions with account context |
| `v_tags` | Tags (join `tag_metadata` for custom attributes) |
| `v_events` | Event directives |
| `v_queries` | Named BQL queries defined in the ledger |
| `v_custom` | Custom directives |
| `v_documents` | Document directives with account context |
| `v_notes` | Note directives with account context |

**Type conventions:**
- Amounts: `DECIMAL(19,9)` — no casting required
- Dates: `DATE` — native date arithmetic and comparison
- Tags/links: comma-separated `VARCHAR` string

## Semantic layer

Accounts and tags carry human-readable labels, exposed as columns in the views.

**Accounts** — set a `label` key on `open` directives in your `.bean` file:

```beancount
2020-01-01 open Assets:Checking:Primary USD
  label: "Primary Checking"

2020-01-01 open Expenses:Groceries USD
  label: "Groceries"
```

These surface as `account_label` in all posting views.

**Tags** — provide a YAML file via `--tags-yaml`. All keys under each tag name are stored as rows in `tag_metadata` and can be queried or pivoted freely:

```yaml
tags:
  vacation-2024:
    label: "Summer 2024 vacation"
    category: "Travel"
```

## Extending

Use `--post-sql` to add custom views or derived tables without modifying this package. Register custom views in `schema_description` to document them:

```sql
-- custom_views.sql
CREATE VIEW v_monthly_spending AS
SELECT
    date_trunc('month', "date") AS month,
    account_label,
    SUM(amount_number) AS total
FROM v_spending
GROUP BY 1, 2;

INSERT INTO schema_description (object_type, name, description)
VALUES ('view', 'v_monthly_spending', 'Monthly spending totals by account label.');
```

## Schema

All Beancount directives are stored in normalized tables.

| Tables | Source directive |
|---|---|
| `account`, `account_category`, `account_currency` | `open` / `close` |
| `transaction`, `posting`, `tag`, `link` | `txn` |
| `assertion` | `balance` |
| `price` | `price` |
| `commodity` | `commodity` |
| `document`, `note`, `event`, `query`, `custom` | remaining directives |
| `*_metadata` | per-directive key/value metadata |

Metadata value types: `str`, `bool`, `date`, `decimal`, `amount`, `null`.

## Development

```bash
uv sync
uv run beancount-duckdb load example.beancount ledger.duckdb
uv run ruff check && uv run ruff format
uv run ty check
uv run sqlfluff lint beancount_duckdb/schema.sql beancount_duckdb/views.sql
```
