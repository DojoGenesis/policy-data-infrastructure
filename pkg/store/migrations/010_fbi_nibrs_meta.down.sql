-- Rollback: 009_fbi_nibrs_meta
-- Removes FBI NIBRS indicator metadata entries.

DELETE FROM indicator_meta WHERE source_id = 'fbi-nibrs';
DELETE FROM indicator_sources WHERE source_id = 'fbi-nibrs';
