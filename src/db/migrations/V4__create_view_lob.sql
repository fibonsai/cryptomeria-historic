DROP VIEW IF EXISTS 'lob';
CREATE VIEW 'lob' AS ( 
WITH dedup_levels AS (
    -- Step 1: remove duplicate rows per level, keep the latest one
    SELECT snapshot_id, ts, side, level, price, size
    FROM (
        SELECT
            snapshot_id, ts, side, level, price, size,
            row_number() OVER (
                PARTITION BY snapshot_id, ts, side, level
                ORDER BY level -- swap for an ingestion/version ts if you have one, e.g. ORDER BY update_ts DESC
            ) AS rn
        FROM lob_levels
    )
    WHERE rn = 1
),
sorted_levels AS (
    -- Step 2: sort so array_agg consumes rows in level order
    SELECT snapshot_id, ts, side, price, size
    FROM dedup_levels
    ORDER BY snapshot_id, ts, side, level ASC
),
bids_agg AS (
    SELECT
        snapshot_id, ts,
        transpose(ARRAY[array_agg(price), array_agg(size)]) AS bids
    FROM sorted_levels
    WHERE side = 'bid'
    GROUP BY snapshot_id, ts
),
asks_agg AS (
    SELECT
        snapshot_id, ts,
        transpose(ARRAY[array_agg(price), array_agg(size)]) AS asks
    FROM sorted_levels
    WHERE side = 'ask'
    GROUP BY snapshot_id, ts
)
SELECT
    s.inst_id,
    s.exchange,
    s.ts,
    s.snapshot_id,
    b.bids,
    a.asks
FROM lob_snapshots s
LEFT JOIN bids_agg b ON s.snapshot_id = b.snapshot_id AND s.ts = b.ts
LEFT JOIN asks_agg a ON s.snapshot_id = a.snapshot_id AND s.ts = a.ts
ORDER BY s.ts DESC
);
