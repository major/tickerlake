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
    volume double precision NOT NULL, transactions bigint NOT NULL,
    sma_20 real, sma_50 real, sma_200 real, atr_14 real, atr_pct real, adr_pct real,
    volume_sma_20 double precision,
    PRIMARY KEY (ticker_id, date),
    CHECK (open NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (high NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (low NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (close NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume >= 0),
    CHECK (transactions >= 0),
    CHECK (high >= open AND high >= close AND high >= low),
    CHECK (low <= open AND low <= close AND low <= high),
    CHECK (sma_20 IS NULL OR sma_20 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_50 IS NULL OR sma_50 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_200 IS NULL OR sma_200 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_14 IS NULL OR atr_14 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_pct IS NULL OR atr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (adr_pct IS NULL OR adr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume_sma_20 IS NULL OR (volume_sma_20 NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume_sma_20 >= 0))
);
CREATE TABLE market.adjusted_weekly (
    ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL, transactions bigint NOT NULL,
    sma_20 real, sma_50 real, sma_200 real, atr_14 real, atr_pct real, adr_pct real,
    volume_sma_20 double precision,
    left_truncated boolean NOT NULL, calendar_closed boolean NOT NULL,
    PRIMARY KEY (ticker_id, date),
    CHECK (open NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (high NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (low NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (close NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume >= 0),
    CHECK (transactions >= 0),
    CHECK (high >= open AND high >= close AND high >= low),
    CHECK (low <= open AND low <= close AND low <= high),
    CHECK (sma_20 IS NULL OR sma_20 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_50 IS NULL OR sma_50 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_200 IS NULL OR sma_200 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_14 IS NULL OR atr_14 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_pct IS NULL OR atr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (adr_pct IS NULL OR adr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume_sma_20 IS NULL OR (volume_sma_20 NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume_sma_20 >= 0))
);
CREATE TABLE market.adjusted_monthly (
    ticker_id integer NOT NULL REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL, transactions bigint NOT NULL,
    sma_20 real, sma_50 real, sma_200 real, atr_14 real, atr_pct real, adr_pct real,
    volume_sma_20 double precision,
    left_truncated boolean NOT NULL, calendar_closed boolean NOT NULL,
    PRIMARY KEY (ticker_id, date),
    CHECK (open NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (high NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (low NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (close NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume >= 0),
    CHECK (transactions >= 0),
    CHECK (high >= open AND high >= close AND high >= low),
    CHECK (low <= open AND low <= close AND low <= high),
    CHECK (sma_20 IS NULL OR sma_20 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_50 IS NULL OR sma_50 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_200 IS NULL OR sma_200 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_14 IS NULL OR atr_14 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_pct IS NULL OR atr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (adr_pct IS NULL OR adr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume_sma_20 IS NULL OR (volume_sma_20 NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume_sma_20 >= 0))
);

CREATE TABLE market.latest_daily (
    ticker_id integer PRIMARY KEY REFERENCES market.ticker(ticker_id),
    date date NOT NULL,
    open real NOT NULL, high real NOT NULL, low real NOT NULL, close real NOT NULL,
    volume double precision NOT NULL, transactions bigint NOT NULL,
    sma_20 real, sma_50 real, sma_200 real, atr_14 real, atr_pct real, adr_pct real,
    volume_sma_20 double precision,
    CHECK (open NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (high NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (low NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (close NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume >= 0),
    CHECK (transactions >= 0),
    CHECK (high >= open AND high >= close AND high >= low),
    CHECK (low <= open AND low <= close AND low <= high),
    CHECK (sma_20 IS NULL OR sma_20 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_50 IS NULL OR sma_50 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (sma_200 IS NULL OR sma_200 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_14 IS NULL OR atr_14 NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (atr_pct IS NULL OR atr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (adr_pct IS NULL OR adr_pct NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real)),
    CHECK (volume_sma_20 IS NULL OR (volume_sma_20 NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) AND volume_sma_20 >= 0))
);

CREATE TABLE market.publication_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
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
