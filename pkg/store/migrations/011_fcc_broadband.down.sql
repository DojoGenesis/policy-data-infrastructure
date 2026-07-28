-- 009_fcc_broadband.down.sql
-- Revert FCC Broadband (Form 477) indicator metadata.

DELETE FROM indicator_meta WHERE source_id = 'fcc-broadband';
DELETE FROM indicator_sources WHERE source_id = 'fcc-broadband';
