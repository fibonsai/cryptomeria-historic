CREATE TABLE IF NOT EXISTS lob_snapshots (
    snapshot_id     LONG,
    ts              TIMESTAMP,
    inst_id         SYMBOL INDEX TYPE POSTING,
    exchange        SYMBOL INDEX TYPE POSTING,
    sequence        LONG,
    best_bid_price  DOUBLE,
    best_bid_size   DOUBLE,
    best_ask_price  DOUBLE,
    best_ask_size   DOUBLE
) TIMESTAMP(ts)
PARTITION BY HOUR
TTL 25 HOURS
WAL;
