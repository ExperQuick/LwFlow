import pytest
import json
import hashlib
from lwflow.utils import hash_args  # replace with your actual import


def test_hash_args_basic():
    args = {"a": 1, "b": 2}
    h = hash_args(args)
    expected = hashlib.sha256(json.dumps(args, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert h == expected


def test_hash_args_order_independence():
    args1 = {"b": 2, "a": 1}
    args2 = {"a": 1, "b": 2}
    assert hash_args(args1) == hash_args(args2)


def test_hash_args_nested_dict():
    args = {"a": {"x": 10, "y": 20}, "b": [1, 2, 3]}
    h = hash_args(args)
    expected = hashlib.sha256(json.dumps(args, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert h == expected


def test_hash_args_non_serializable():
    class NonSerializable:
        pass

    args = {"a": NonSerializable()}
    with pytest.raises(TypeError):
        hash_args(args)
