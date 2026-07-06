"""Tests for plf.utils helper functions."""

from plf.utils import hash_args


def test_hash_args_is_deterministic():
    """Same input dict must always produce the same SHA-256 hash."""
    config = {"workflow": {"loc": "pkg.WF"}, "args": {"lr": 0.01}}
    first = hash_args(config)
    second = hash_args(config)
    assert first == second


def test_hash_args_key_order_independent():
    """JSON sorting ensures key order does not change the hash."""
    config_a = {"b": 2, "a": 1}
    config_b = {"a": 1, "b": 2}
    assert hash_args(config_a) == hash_args(config_b)


def test_hash_args_detects_value_changes():
    """Different values must produce different hashes."""
    base = {"args": {"epochs": 10}}
    changed = {"args": {"epochs": 20}}
    assert hash_args(base) != hash_args(changed)


def test_hash_args_returns_hex_string():
    """Hash output should be a 64-character lowercase hex string."""
    result = hash_args({"key": "value"})
    assert isinstance(result, str)
    assert len(result) == 64
    assert all(ch in "0123456789abcdef" for ch in result)
