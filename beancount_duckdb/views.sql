-- SQL views for beancount-duckdb analytics layer.
-- These are the intended query surface — only query views, never raw tables.
-- Each view flattens joins and metadata so callers get clean, named columns.
--
-- Rebuild strategy: DROP + CREATE so this script is idempotent.
-- Drop order: most-dependent views first.

DROP VIEW IF EXISTS v_spending;
DROP VIEW IF EXISTS v_income;
DROP VIEW IF EXISTS v_postings;
DROP VIEW IF EXISTS v_assertions;
DROP VIEW IF EXISTS v_documents;
DROP VIEW IF EXISTS v_notes;
DROP VIEW IF EXISTS v_prices;
DROP VIEW IF EXISTS v_commodities;
DROP VIEW IF EXISTS v_transactions;
DROP VIEW IF EXISTS v_tags;
DROP VIEW IF EXISTS v_events;
DROP VIEW IF EXISTS v_queries;
DROP VIEW IF EXISTS v_custom;
DROP VIEW IF EXISTS v_accounts;

-- v_accounts: accounts with label from open_metadata.
CREATE VIEW v_accounts AS
SELECT
    a.id,
    a.name,
    a.account_type,
    om_label."value" AS label,
    a.open_date,
    a.close_date
FROM account AS a
LEFT JOIN open_metadata AS om_label
    ON
        a.id = om_label.account_id
        AND om_label."key" = 'label';

-- v_commodities: commodities with common metadata keys pivoted as columns.
CREATE VIEW v_commodities AS
SELECT
    c.id,
    c."date",
    c.currency,
    c.decimal_places,
    cm_name."value" AS name,
    cm_class."value" AS asset_class,
    cm_subclass."value" AS asset_subclass,
    cm_quote."value" AS quote
FROM commodity AS c
LEFT JOIN commodity_metadata AS cm_name
    ON
        c.id = cm_name.commodity_id
        AND cm_name."key" = 'name'
LEFT JOIN commodity_metadata AS cm_class
    ON
        c.id = cm_class.commodity_id
        AND cm_class."key" = 'asset-class'
LEFT JOIN commodity_metadata AS cm_subclass
    ON
        c.id = cm_subclass.commodity_id
        AND cm_subclass."key" = 'asset-subclass'
LEFT JOIN commodity_metadata AS cm_quote
    ON
        c.id = cm_quote.commodity_id
        AND cm_quote."key" = 'quote';

-- v_tags: tags. Join tag_metadata for custom attributes.
CREATE VIEW v_tags AS
SELECT
    id,
    name
FROM tag;

-- v_transactions: transactions with comma-separated tags and links.
CREATE VIEW v_transactions AS
SELECT
    t.id,
    t."date",
    t.flag,
    t.payee,
    t.narration,
    (
        SELECT string_agg(tg.name, ',')
        FROM transaction_tag AS tt
        INNER JOIN tag AS tg ON tt.tag_id = tg.id
        WHERE tt.transaction_id = t.id
    ) AS tags,
    (
        SELECT string_agg(lk.name, ',')
        FROM transaction_link AS tl
        INNER JOIN link AS lk ON tl.link_id = lk.id
        WHERE tl.transaction_id = t.id
    ) AS links
FROM "transaction" AS t;

-- v_events: life events (job changes, moves, etc.).
CREATE VIEW v_events AS
SELECT
    id,
    "date",
    type,
    description
FROM event;

-- v_queries: named BQL queries defined in the ledger.
CREATE VIEW v_queries AS
SELECT
    id,
    "date",
    name,
    query_string
FROM "query";

-- v_custom: custom directives (Fava config, plugin settings, etc.).
CREATE VIEW v_custom AS
SELECT
    id,
    "date",
    type,
    "values"
FROM "custom";

-- v_postings: all postings with account and transaction context.
-- Joins v_accounts so account_label is included directly.
CREATE VIEW v_postings AS
SELECT
    p.id AS posting_id,
    t."date",
    t.flag,
    t.payee,
    t.narration,
    a.name AS account,
    a.account_type,
    a.label AS account_label,
    p.amount_number,
    p.amount_currency,
    (
        SELECT string_agg(tg.name, ',')
        FROM transaction_tag AS tt
        INNER JOIN tag AS tg ON tt.tag_id = tg.id
        WHERE tt.transaction_id = t.id
    ) AS tags
FROM posting AS p
INNER JOIN "transaction" AS t ON p.transaction_id = t.id
INNER JOIN v_accounts AS a ON p.account_id = a.id;

-- v_prices: price entries with commodity display name.
CREATE VIEW v_prices AS
SELECT
    p.id,
    p."date",
    p.currency,
    vc.name AS commodity_name,
    p.amount_number,
    p.amount_currency
FROM price AS p
LEFT JOIN v_commodities AS vc ON p.currency = vc.currency;

-- v_assertions: balance assertions with account context.
CREATE VIEW v_assertions AS
SELECT
    a.id,
    a."date",
    va.name AS account,
    va.account_type,
    va.label AS account_label,
    a.amount_number,
    a.amount_currency
FROM assertion AS a
INNER JOIN v_accounts AS va ON a.account_id = va.id;

-- v_documents: document directives with account context.
CREATE VIEW v_documents AS
SELECT
    d.id,
    d."date",
    va.name AS account,
    va.account_type,
    va.label AS account_label,
    d.filename
FROM document AS d
INNER JOIN v_accounts AS va ON d.account_id = va.id;

-- v_notes: note directives with account context.
CREATE VIEW v_notes AS
SELECT
    n.id,
    n."date",
    va.name AS account,
    va.account_type,
    va.label AS account_label,
    n.comment
FROM note AS n
INNER JOIN v_accounts AS va ON n.account_id = va.id;

-- v_spending: expense postings. Filtered subset of v_postings.
CREATE VIEW v_spending AS
SELECT
    posting_id,
    "date",
    flag,
    payee,
    narration,
    account,
    account_type,
    account_label,
    amount_number,
    amount_currency,
    tags
FROM v_postings
WHERE account_type = 'Expenses';

-- v_income: income postings. Filtered subset of v_postings.
-- Note: amount_number is typically negative. Use ABS() for magnitudes.
CREATE VIEW v_income AS
SELECT
    posting_id,
    "date",
    flag,
    payee,
    narration,
    account,
    account_type,
    account_label,
    amount_number,
    amount_currency,
    tags
FROM v_postings
WHERE account_type = 'Income';
