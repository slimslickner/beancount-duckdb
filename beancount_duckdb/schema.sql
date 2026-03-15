-- DuckDB schema for beancount-duckdb
-- Dates are stored as DATE.
-- Numeric amounts are stored as DECIMAL(19,9) to preserve precision.

-- Sequences for auto-increment primary keys
CREATE SEQUENCE IF NOT EXISTS seq_account_category;
CREATE SEQUENCE IF NOT EXISTS seq_account;
CREATE SEQUENCE IF NOT EXISTS seq_transaction;
CREATE SEQUENCE IF NOT EXISTS seq_tag;
CREATE SEQUENCE IF NOT EXISTS seq_link;
CREATE SEQUENCE IF NOT EXISTS seq_posting;
CREATE SEQUENCE IF NOT EXISTS seq_commodity;
CREATE SEQUENCE IF NOT EXISTS seq_price;
CREATE SEQUENCE IF NOT EXISTS seq_assertion;
CREATE SEQUENCE IF NOT EXISTS seq_document;
CREATE SEQUENCE IF NOT EXISTS seq_note;
CREATE SEQUENCE IF NOT EXISTS seq_event;
CREATE SEQUENCE IF NOT EXISTS seq_query;
CREATE SEQUENCE IF NOT EXISTS seq_custom;
CREATE SEQUENCE IF NOT EXISTS seq_tag_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_transaction_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_posting_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_open_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_close_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_commodity_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_balance_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_note_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_document_metadata;
CREATE SEQUENCE IF NOT EXISTS seq_price_metadata;

-- Account category table (hierarchical account tree)
CREATE TABLE IF NOT EXISTS account_category (
    id INTEGER DEFAULT nextval('seq_account_category') PRIMARY KEY,
    name VARCHAR NOT NULL,
    parent_id INTEGER,
    account_type VARCHAR NOT NULL CHECK (
        account_type IN (
            'Assets', 'Liabilities', 'Equity', 'Income', 'Expenses'
        )
    ),
    FOREIGN KEY (parent_id) REFERENCES account_category (id),
    UNIQUE (name, parent_id, account_type)
);

-- Account table (Open/Close directives)
CREATE TABLE IF NOT EXISTS account (
    id INTEGER DEFAULT nextval('seq_account') PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    account_type VARCHAR NOT NULL CHECK (
        account_type IN (
            'Assets', 'Liabilities', 'Equity', 'Income', 'Expenses'
        )
    ),
    account_category_id INTEGER NOT NULL,
    open_date DATE NOT NULL,
    close_date DATE,
    FOREIGN KEY (account_category_id) REFERENCES account_category (id)
);

-- Currencies declared on an account (Open.currencies)
CREATE TABLE IF NOT EXISTS account_currency (
    account_id INTEGER NOT NULL,
    currency VARCHAR NOT NULL,
    PRIMARY KEY (account_id, currency),
    FOREIGN KEY (account_id) REFERENCES account (id)
);

-- Transaction table
CREATE TABLE IF NOT EXISTS "transaction" (
    id INTEGER DEFAULT nextval('seq_transaction') PRIMARY KEY,
    "date" DATE NOT NULL,
    flag VARCHAR NOT NULL,
    payee VARCHAR NOT NULL,
    narration VARCHAR NOT NULL
);

-- Tag table
CREATE TABLE IF NOT EXISTS tag (
    id INTEGER DEFAULT nextval('seq_tag') PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE
);

-- Transaction <-> tag junction
CREATE TABLE IF NOT EXISTS transaction_tag (
    transaction_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (transaction_id, tag_id),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id),
    FOREIGN KEY (tag_id) REFERENCES tag (id)
);

-- Link table
CREATE TABLE IF NOT EXISTS link (
    id INTEGER DEFAULT nextval('seq_link') PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE
);

-- Transaction <-> link junction
CREATE TABLE IF NOT EXISTS transaction_link (
    transaction_id INTEGER NOT NULL,
    link_id INTEGER NOT NULL,
    PRIMARY KEY (transaction_id, link_id),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id),
    FOREIGN KEY (link_id) REFERENCES link (id)
);

-- Posting table
CREATE TABLE IF NOT EXISTS posting (
    id INTEGER DEFAULT nextval('seq_posting') PRIMARY KEY,
    "date" DATE NOT NULL,
    account_id INTEGER NOT NULL,
    transaction_id INTEGER NOT NULL,
    flag VARCHAR,
    amount_number DECIMAL(19,9) NOT NULL,
    amount_currency VARCHAR NOT NULL,
    price_number DECIMAL(19,9),
    price_currency VARCHAR,
    cost_number DECIMAL(19,9),
    cost_currency VARCHAR,
    cost_date DATE,
    cost_label VARCHAR,
    matching_lot_id INTEGER,
    FOREIGN KEY (account_id) REFERENCES account (id),
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id),
    FOREIGN KEY (matching_lot_id) REFERENCES posting (id)
);

CREATE INDEX IF NOT EXISTS posting_account_id_date_idx ON posting (
    account_id, "date", id
);
CREATE INDEX IF NOT EXISTS posting_transaction_id_idx ON posting (
    transaction_id
);

-- Commodity table
CREATE TABLE IF NOT EXISTS commodity (
    id INTEGER DEFAULT nextval('seq_commodity') PRIMARY KEY,
    "date" DATE NOT NULL,
    currency VARCHAR NOT NULL UNIQUE CHECK (currency != ''),
    decimal_places INTEGER NOT NULL DEFAULT 0
);

-- Price table
CREATE TABLE IF NOT EXISTS price (
    id INTEGER DEFAULT nextval('seq_price') PRIMARY KEY,
    "date" DATE NOT NULL,
    currency VARCHAR NOT NULL,
    amount_number DECIMAL(19,9) NOT NULL,
    amount_currency VARCHAR NOT NULL
);

-- Assertion table (Balance directives)
CREATE TABLE IF NOT EXISTS assertion (
    id INTEGER DEFAULT nextval('seq_assertion') PRIMARY KEY,
    "date" DATE NOT NULL,
    account_id INTEGER NOT NULL,
    amount_number DECIMAL(19,9) NOT NULL,
    amount_currency VARCHAR NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account (id)
);

-- Document table (file path only — no binary storage)
CREATE TABLE IF NOT EXISTS document (
    id INTEGER DEFAULT nextval('seq_document') PRIMARY KEY,
    "date" DATE NOT NULL,
    account_id INTEGER NOT NULL,
    filename VARCHAR NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account (id)
);

-- Note table (Note directives)
CREATE TABLE IF NOT EXISTS note (
    id INTEGER DEFAULT nextval('seq_note') PRIMARY KEY,
    "date" DATE NOT NULL,
    account_id INTEGER NOT NULL,
    comment VARCHAR NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account (id)
);

-- Event table (Event directives)
CREATE TABLE IF NOT EXISTS event (
    id INTEGER DEFAULT nextval('seq_event') PRIMARY KEY,
    "date" DATE NOT NULL,
    type VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

-- Query table (Query directives)
CREATE TABLE IF NOT EXISTS "query" (
    id INTEGER DEFAULT nextval('seq_query') PRIMARY KEY,
    "date" DATE NOT NULL,
    name VARCHAR NOT NULL,
    query_string VARCHAR NOT NULL
);

-- Custom table (Custom directives)
CREATE TABLE IF NOT EXISTS "custom" (
    id INTEGER DEFAULT nextval('seq_custom') PRIMARY KEY,
    "date" DATE NOT NULL,
    type VARCHAR NOT NULL,
    "values" VARCHAR NOT NULL DEFAULT '[]'
);

-- Metadata tables (normalized key/value, one row per metadata entry).
-- value_type encodes the original beancount grammar type:
--   str     → STRING, account name, currency, or tag token
--   bool    → BOOL
--   date    → DATE
--   decimal → number expression (Decimal)
--   amount  → amount literal, stored as "<number> <currency>"
--   null    → NONE or empty value

CREATE TABLE IF NOT EXISTS tag_metadata (
    id INTEGER DEFAULT nextval('seq_tag_metadata') PRIMARY KEY,
    tag_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    UNIQUE (tag_id, "key"),
    FOREIGN KEY (tag_id) REFERENCES tag (id)
);

CREATE TABLE IF NOT EXISTS transaction_metadata (
    id INTEGER DEFAULT nextval('seq_transaction_metadata') PRIMARY KEY,
    transaction_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES "transaction" (id)
);

CREATE TABLE IF NOT EXISTS posting_metadata (
    id INTEGER DEFAULT nextval('seq_posting_metadata') PRIMARY KEY,
    posting_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (posting_id) REFERENCES posting (id)
);

CREATE TABLE IF NOT EXISTS open_metadata (
    id INTEGER DEFAULT nextval('seq_open_metadata') PRIMARY KEY,
    account_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account (id)
);

CREATE TABLE IF NOT EXISTS close_metadata (
    id INTEGER DEFAULT nextval('seq_close_metadata') PRIMARY KEY,
    account_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account (id)
);

CREATE TABLE IF NOT EXISTS commodity_metadata (
    id INTEGER DEFAULT nextval('seq_commodity_metadata') PRIMARY KEY,
    commodity_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (commodity_id) REFERENCES commodity (id)
);

CREATE TABLE IF NOT EXISTS balance_metadata (
    id INTEGER DEFAULT nextval('seq_balance_metadata') PRIMARY KEY,
    assertion_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (assertion_id) REFERENCES assertion (id)
);

CREATE TABLE IF NOT EXISTS note_metadata (
    id INTEGER DEFAULT nextval('seq_note_metadata') PRIMARY KEY,
    note_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (note_id) REFERENCES note (id)
);

CREATE TABLE IF NOT EXISTS document_metadata (
    id INTEGER DEFAULT nextval('seq_document_metadata') PRIMARY KEY,
    document_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (document_id) REFERENCES document (id)
);

CREATE TABLE IF NOT EXISTS price_metadata (
    id INTEGER DEFAULT nextval('seq_price_metadata') PRIMARY KEY,
    price_id INTEGER NOT NULL,
    "key" VARCHAR NOT NULL,
    "value" VARCHAR,
    value_type VARCHAR NOT NULL,
    FOREIGN KEY (price_id) REFERENCES price (id)
);

-- Schema documentation table.
-- Stores human-readable descriptions for tables and views.
-- Seeded by the loader for built-in objects.
-- Extend via --post-sql to describe custom views or override built-in descriptions.
CREATE TABLE IF NOT EXISTS schema_description (
    object_type VARCHAR NOT NULL CHECK (object_type IN ('table', 'view')),
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    PRIMARY KEY (object_type, name)
);
