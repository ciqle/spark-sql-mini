CREATE TABLE raw_events (id INT, event_date DATE, amount INT) USING PARQUET;

INSERT INTO raw_events VALUES
  (1, DATE '2026-11-08', 4),
  (2, DATE '2026-11-08', 10),
  (3, DATE '2026-11-08', 16),
  (4, DATE '2026-11-09', 2),
  (5, DATE '2026-11-09', 8);
