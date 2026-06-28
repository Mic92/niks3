-- +goose up

-- +goose statementbegin
-- Maintain object_stats running totals for the live object set.
--
-- A row contributes (1, size) while alive (deleted_at IS NULL), else (0, 0).
-- Applying contribution(NEW) - contribution(OLD) handles insert, delete,
-- tombstone and resurrect uniformly without branching on the operation.
CREATE OR REPLACE FUNCTION object_stats_apply()
RETURNS trigger AS $$
DECLARE
    d_count bigint := 0;
    d_bytes bigint := 0;
BEGIN
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.deleted_at IS NULL THEN
        d_count := d_count + 1;
        d_bytes := d_bytes + COALESCE(NEW.size, 0);
    END IF;

    IF (TG_OP = 'DELETE' OR TG_OP = 'UPDATE') AND OLD.deleted_at IS NULL THEN
        d_count := d_count - 1;
        d_bytes := d_bytes - COALESCE(OLD.size, 0);
    END IF;

    IF d_count <> 0 OR d_bytes <> 0 THEN
        UPDATE object_stats
        SET object_count = object_count + d_count,
            total_bytes = total_bytes + d_bytes
        WHERE id;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS object_stats_trigger ON objects;
CREATE TRIGGER object_stats_trigger
AFTER INSERT OR UPDATE OR DELETE ON objects
FOR EACH ROW EXECUTE FUNCTION object_stats_apply();
-- +goose statementend
