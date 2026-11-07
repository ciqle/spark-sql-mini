-- Each row has two numeric features. The model computes 2 * x1 - x2 + 0.5.
features = SELECT * FROM VALUES
  (1, 1.0F, 0.25F),
  (2, 3.0F, 5.0F),
  (3, 0.0F, 2.0F)
AS samples(id, x1, x2);

scored = SCORE features USING ONNX 'examples/models/linear.onnx'
  FEATURES (x1, x2) AS model_score;

SELECT id, x1, x2, model_score FROM scored ORDER BY id;
