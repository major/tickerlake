-- Forward-only migration: shared domains for finite OHLC/volume columns plus a
-- single OHLC-relationship helper. Assumes 0001 and 0002 have already been
-- applied, including the removal of the technical-analysis metric columns, so
-- this file only touches columns that still exist afterwards.

CREATE DOMAIN market.finite_real AS real
        CHECK (VALUE NOT IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real));

CREATE DOMAIN market.finite_volume AS double precision
        CHECK (
            VALUE NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)
            AND VALUE >= 0
        );

CREATE FUNCTION market.is_valid_ohlc(open real, high real, low real, close real)
    RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
AS $$
    SELECT high >= open AND high >= close AND high >= low
           AND low <= open AND low <= close AND low <= high
$$;

-- ingest.raw_daily
ALTER TABLE ingest.raw_daily
    DROP CONSTRAINT IF EXISTS raw_daily_open_check,
    DROP CONSTRAINT IF EXISTS raw_daily_high_check,
    DROP CONSTRAINT IF EXISTS raw_daily_low_check,
    DROP CONSTRAINT IF EXISTS raw_daily_close_check,
    DROP CONSTRAINT IF EXISTS raw_daily_volume_check,
    DROP CONSTRAINT IF EXISTS raw_daily_check,
    DROP CONSTRAINT IF EXISTS raw_daily_check1;

ALTER TABLE ingest.raw_daily
    ALTER COLUMN open TYPE market.finite_real,
    ALTER COLUMN high TYPE market.finite_real,
    ALTER COLUMN low TYPE market.finite_real,
    ALTER COLUMN close TYPE market.finite_real,
    ALTER COLUMN volume TYPE market.finite_volume;

ALTER TABLE ingest.raw_daily
    ADD CONSTRAINT raw_daily_ohlc_check CHECK (market.is_valid_ohlc(open, high, low, close));

-- market.adjusted_bars
ALTER TABLE market.adjusted_bars
    DROP CONSTRAINT IF EXISTS adjusted_bars_volume_check,
    DROP CONSTRAINT IF EXISTS adjusted_bars_check,
    DROP CONSTRAINT IF EXISTS adjusted_bars_check1;

ALTER TABLE market.adjusted_bars
    ALTER COLUMN open TYPE market.finite_real,
    ALTER COLUMN high TYPE market.finite_real,
    ALTER COLUMN low TYPE market.finite_real,
    ALTER COLUMN close TYPE market.finite_real,
    ALTER COLUMN volume TYPE market.finite_volume;

ALTER TABLE market.adjusted_bars
    ADD CONSTRAINT adjusted_bars_ohlc_check CHECK (market.is_valid_ohlc(open, high, low, close));

-- market.latest_daily
ALTER TABLE market.latest_daily
    DROP CONSTRAINT IF EXISTS latest_daily_open_check,
    DROP CONSTRAINT IF EXISTS latest_daily_high_check,
    DROP CONSTRAINT IF EXISTS latest_daily_low_check,
    DROP CONSTRAINT IF EXISTS latest_daily_close_check,
    DROP CONSTRAINT IF EXISTS latest_daily_volume_check,
    DROP CONSTRAINT IF EXISTS latest_daily_check,
    DROP CONSTRAINT IF EXISTS latest_daily_check1;

ALTER TABLE market.latest_daily
    ALTER COLUMN open TYPE market.finite_real,
    ALTER COLUMN high TYPE market.finite_real,
    ALTER COLUMN low TYPE market.finite_real,
    ALTER COLUMN close TYPE market.finite_real,
    ALTER COLUMN volume TYPE market.finite_volume;

ALTER TABLE market.latest_daily
    ADD CONSTRAINT latest_daily_ohlc_check CHECK (market.is_valid_ohlc(open, high, low, close));
