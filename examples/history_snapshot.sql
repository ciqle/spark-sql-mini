CREATE TABLE status_history (entity_id INT, effective_on DATE, revision INT, status STRING, ingested_on DATE)
  USING PARQUET PARTITIONED BY (ingested_on);

-- A correction arrived after its effective date. A scheduled state arrived early.
changes = SELECT * FROM VALUES
  (10, DATE '2026-11-03', 1, 'draft',     DATE '2026-11-05'),
  (10, DATE '2026-11-06', 1, 'active',    DATE '2026-11-08'),
  (10, DATE '2026-11-06', 2, 'verified',  DATE '2026-11-08'),
  (10, DATE '2026-11-09', 1, 'scheduled', DATE '2026-11-08'),
  (20, DATE '2026-11-03', 1, 'legacy',    DATE '2026-11-04'),
  (30, DATE '2026-11-05', 1, 'active',    DATE '2026-11-05')
AS changes(entity_id, effective_on, revision, status, ingested_on);
INSERT changes INTO TABLE status_history PARTITION (ingested_on);

daily_state = SNAPSHOT status_history KEY (entity_id)
  AS OF DATE '2026-11-07' USING effective_on
  TIE BREAK BY revision
  READ PARTITIONS ingested_on FROM DATE '2026-11-05' TO DATE '2026-11-08';

SELECT entity_id, status, effective_on, ingested_on FROM daily_state ORDER BY entity_id;
EXPLAIN FORMATTED SELECT * FROM daily_state;

samples = SELECT * FROM VALUES
  (1, 10, DATE '2026-11-06'),
  (2, 10, DATE '2026-11-09'),
  (3, 30, DATE '2026-11-08'),
  (4, 20, DATE '2026-11-07')
AS samples(sample_id, entity_id, sample_on);

enriched = PIT JOIN samples WITH status_history ON (entity_id = entity_id)
  AS OF sample_on USING effective_on
  LOOKBACK 2 DAYS
  TIE BREAK BY revision
  READ PARTITIONS ingested_on FROM DATE '2026-11-05' TO DATE '2026-11-08';

SELECT sample_id, history_status FROM enriched ORDER BY sample_id;
