CREATE INDEX raw_daily_ticker_date_idx ON ingest.raw_daily (ticker_id, date);
CREATE INDEX split_event_ticker_idx ON ingest.split_event (ticker_id);

CREATE TABLE ingest.raw_session (
    date date PRIMARY KEY,
    manifest_id bigint NOT NULL REFERENCES ingest.fetch_manifest(manifest_id)
);

CREATE TABLE market.adjusted_bars (
    period text NOT NULL CHECK (period IN ('daily', 'weekly', 'monthly')),
    ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL,
    left_truncated boolean NOT NULL,
    calendar_closed boolean NOT NULL,
    PRIMARY KEY (period, ticker_id, date),
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

GRANT SELECT, INSERT, UPDATE, DELETE ON ingest.raw_session TO tickerlake_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON market.adjusted_bars,
    market.latest_daily, market.publication_state TO tickerlake_etl;
GRANT SELECT ON market.adjusted_bars,
    market.latest_daily, market.publication_state TO tickerlake_reader;
