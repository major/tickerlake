CREATE INDEX raw_daily_ticker_date_idx ON ingest.raw_daily (ticker_id, date);
CREATE INDEX split_event_ticker_idx ON ingest.split_event (ticker_id);

CREATE TABLE ingest.raw_session (
    date date PRIMARY KEY,
    input_revision bigint NOT NULL CHECK (input_revision >= 0),
    manifest_id bigint NOT NULL REFERENCES ingest.fetch_manifest(manifest_id),
    row_count bigint NOT NULL CHECK (row_count > 0)
);

CREATE TABLE market.adjusted_daily (
    ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL,
    PRIMARY KEY (ticker_id, date),
    CHECK (volume >= 0),
    CHECK (high >= GREATEST(open, close, low) AND low <= LEAST(open, close, high))
);
CREATE TABLE market.adjusted_weekly (
    ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL,
    left_truncated boolean NOT NULL, calendar_closed boolean NOT NULL,
    PRIMARY KEY (ticker_id, date),
    CHECK (volume >= 0),
    CHECK (high >= GREATEST(open, close, low) AND low <= LEAST(open, close, high))
);
CREATE TABLE market.adjusted_monthly (
    ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL,
    left_truncated boolean NOT NULL, calendar_closed boolean NOT NULL,
    PRIMARY KEY (ticker_id, date),
    CHECK (volume >= 0),
    CHECK (high >= GREATEST(open, close, low) AND low <= LEAST(open, close, high))
);

CREATE TABLE market.latest_daily (
    ticker_id integer PRIMARY KEY REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL,
    CHECK (volume >= 0),
    CHECK (high >= GREATEST(open, close, low) AND low <= LEAST(open, close, high))
);

CREATE TABLE market.publication_state (
    publication_state_id integer PRIMARY KEY DEFAULT 1 CHECK (publication_state_id = 1),
    published_session date NOT NULL,
    published_at timestamptz NOT NULL,
    run_id uuid NOT NULL REFERENCES ingest.run(run_id),
    ticker_count bigint NOT NULL CHECK (ticker_count >= 0)
);

REVOKE ALL ON ingest.raw_session FROM PUBLIC, tickerlake_etl, tickerlake_reader;
REVOKE ALL ON market.adjusted_daily, market.adjusted_weekly, market.adjusted_monthly,
    market.latest_daily, market.publication_state FROM PUBLIC, tickerlake_etl, tickerlake_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ingest.raw_session TO tickerlake_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON market.adjusted_daily, market.adjusted_weekly,
    market.adjusted_monthly, market.latest_daily, market.publication_state TO tickerlake_etl;
GRANT SELECT ON market.adjusted_daily, market.adjusted_weekly, market.adjusted_monthly,
    market.latest_daily, market.publication_state TO tickerlake_reader;
