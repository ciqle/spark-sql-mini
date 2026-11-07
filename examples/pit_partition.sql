-- Two samples for the same customer, and one customer with no history.
samples = SELECT * FROM VALUES
  (1, 10, TIMESTAMP '2026-11-07 10:00:00'),
  (2, 10, TIMESTAMP '2026-11-07 10:30:00'),
  (3, 20, TIMESTAMP '2026-11-07 10:30:00')
AS samples(sample_id, customer_id, sample_ts);

history = SELECT * FROM VALUES
  (10, TIMESTAMP '2026-11-07 09:00:00', 1, 0.25D),
  (10, TIMESTAMP '2026-11-07 10:15:00', 1, 0.60D),
  (10, TIMESTAMP '2026-11-07 10:15:00', 2, 0.65D),
  (10, TIMESTAMP '2026-11-07 11:00:00', 1, 0.99D)
AS history(entity_id, feature_ts, revision, risk_feature);

enriched = PIT JOIN samples WITH history
  ON (customer_id = entity_id)
  AS OF sample_ts USING feature_ts
  TIE BREAK BY revision;

-- A static partition value is supplied by INSERT, so the source omits dt.
features_to_save = SELECT sample_id, history_risk_feature FROM enriched;
CREATE TABLE daily_features (sample_id INT, risk_feature DOUBLE, dt DATE)
  USING PARQUET PARTITIONED BY (dt);
INSERT features_to_save INTO TABLE daily_features PARTITION (dt = DATE '2026-11-07');

SELECT sample_id, risk_feature, dt FROM daily_features ORDER BY sample_id;
SHOW PARTITIONS daily_features;
