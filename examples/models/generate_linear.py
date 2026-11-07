# /// script
# requires-python = ">=3.10"
# dependencies = ["onnx==1.19.1"]
# ///
"""Regenerate linear.onnx with `uv run --no-project examples/models/generate_linear.py`."""

from pathlib import Path

import numpy as np
import onnx
from onnx import TensorProto, helper
from onnx.reference import ReferenceEvaluator


features = helper.make_tensor_value_info("features", TensorProto.FLOAT, ["batch", 2])
score = helper.make_tensor_value_info("score", TensorProto.FLOAT, ["batch", 1])
weights = helper.make_tensor("weights", TensorProto.FLOAT, [2, 1], [2.0, -1.0])
bias = helper.make_tensor("bias", TensorProto.FLOAT, [1], [0.5])
graph = helper.make_graph(
    [helper.make_node("Gemm", ["features", "weights", "bias"], ["score"])],
    "linear_score",
    [features],
    [score],
    [weights, bias],
)
model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)], ir_version=8)

onnx.checker.check_model(model, full_check=True)
sample = np.array([[1.0, 0.25], [3.0, 5.0], [0.0, 2.0]], dtype=np.float32)
expected = np.array([[2.25], [1.5], [-1.5]], dtype=np.float32)
np.testing.assert_array_equal(ReferenceEvaluator(model).run(None, {"features": sample})[0], expected)
onnx.save_model(model, Path(__file__).with_name("linear.onnx"))
