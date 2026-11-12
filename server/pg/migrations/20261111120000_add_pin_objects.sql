-- +goose Up
ALTER TABLE pins
ADD COLUMN previous_narinfo_key varchar(1024),
ADD COLUMN moved_at timestamptz;

CREATE TABLE pin_objects (
    pin varchar(256) NOT NULL REFERENCES pins (name) ON DELETE CASCADE,
    narinfo_key varchar(1024) NOT NULL,
    key varchar(1024) NOT NULL,
    PRIMARY KEY (pin, key, narinfo_key)
);

-- Backfill pin_objects for existing pins using recursive CTE
WITH RECURSIVE reach (pin, narinfo_key, key) AS (
    SELECT
        name,
        narinfo_key,
        narinfo_key::varchar
    FROM pins
    UNION
    SELECT
        reach.pin,
        reach.narinfo_key,
        ref
    FROM reach
    JOIN objects o ON o.key = reach.key
    CROSS JOIN LATERAL unnest(o.refs) AS ref
)

INSERT INTO pin_objects (pin, narinfo_key, key)
SELECT
    pin,
    narinfo_key,
    key
FROM reach
ON CONFLICT DO NOTHING;

-- +goose Down
DROP TABLE pin_objects;
ALTER TABLE pins
DROP COLUMN moved_at,
DROP COLUMN previous_narinfo_key;
