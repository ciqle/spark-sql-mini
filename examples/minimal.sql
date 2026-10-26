-- Native Spark SQL creates the target table; the wrapper does not invent a catalog.
CREATE TABLE demo_output (id BIGINT, label STRING) USING PARQUET;

-- Extension 1: a DataFrame is registered as a session-local temp view.
temp = SELECT id, concat('item-', cast(id AS STRING)) AS label
       FROM range(5) WHERE id >= 2;

-- A second temp view can reuse the first without materializing it here.
selected = SELECT * FROM temp WHERE id < 4;

-- Extension 2: append the temp view to an existing Spark table.
INSERT selected INTO TABLE demo_output;

-- Native queries still go to Spark; the command-line runner displays their rows.
SELECT * FROM demo_output ORDER BY id;

-- Inspect the plan with Spark's own command.
EXPLAIN FORMATTED SELECT * FROM selected;
