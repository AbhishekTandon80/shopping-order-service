-- You might want to use these drop statements anytime or never

-- Drop user table
DROP TABLE IF EXISTS item_popularity;

--Drop akka tables
DROP INDEX IF EXISTS event_journal_slice_idx;

DROP TABLE IF EXISTS event_journal;

DROP INDEX IF EXISTS snapshot_slice_idx;

DROP TABLE IF EXISTS snapshot;

DROP INDEX IF EXISTS durable_state_slice_idx;

DROP TABLE IF EXISTS durable_state;

DROP TABLE IF EXISTS akka_projection_offset_store;

DROP TABLE IF EXISTS akka_projection_timestamp_offset_store;

DROP TABLE IF EXISTS akka_projection_management;