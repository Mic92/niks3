-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cleanup_pending_closures(
    seconds int
) RETURNS void AS $$
BEGIN
    -- Create a temporary view for old_pending_closures
    EXECUTE format('
        CREATE TEMP VIEW temp_old_pending_closures AS
        SELECT id
        FROM pending_closures
        WHERE started_at < NOW() - interval ''1 second'' * %L', seconds);

    -- Insert pending objects into objects table if they don't already exist
    INSERT INTO objects (key, deleted_at)
    SELECT po.key, NOW()
    FROM pending_objects po
    JOIN temp_old_pending_closures opc ON po.pending_closure_id = opc.id
    ON CONFLICT (key) DO NOTHING;

    -- Delete pending objects whose associated pending closures are older than the specified interval
    DELETE FROM pending_objects
    WHERE pending_closure_id IN (SELECT id FROM temp_old_pending_closures);

    -- Delete pending closures older than the specified interval
    DELETE FROM pending_closures
    WHERE id IN (SELECT id FROM temp_old_pending_closures);

    -- Drop the temporary view
    DROP VIEW temp_old_pending_closures;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend
