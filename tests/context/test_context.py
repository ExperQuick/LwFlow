import os
import sys
import tempfile
import types
import pytest

from lwflow import context


def test_get_set_shared_data_is_isolated():
    # Clear internal state
    context._CONTEXT_DATA.clear()

    # Set shared data
    test_data = {"project": "lab", "version": 1}
    updated = context.set_shared_data(test_data.copy(), logid="log123")

    # Ensure the context holds the data + logid
    assert updated["project"] == "lab"
    assert updated["version"] == 1
    assert updated["logid"] == "log123"

    # Retrieve it and confirm it's the same
    shared = context.get_shared_data()
    assert shared == updated

def test_set_shared_data_with_non_dict():
    context._CONTEXT_DATA.clear()

    # Non-dict input
    updated = context.set_shared_data(data="something", logid="log456")
    assert updated == {"logid": "log456"}

def test_context_is_isolated_between_roots():
    context._CONTEXT_DATA.clear()

    # Simulate two contexts with different root frames
    fake_context1 = "/fake/context1.py"
    fake_context2 = "/fake/context2.py"

    context._CONTEXT_DATA[fake_context1] = {"a": 1}
    context._CONTEXT_DATA[fake_context2] = {"b": 2}

    assert context._CONTEXT_DATA[fake_context1] == {"a": 1}
    assert context._CONTEXT_DATA[fake_context2] == {"b": 2}

def test_register_libs_path_adds_valid_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Directory should not be in sys.path
        assert tmpdir not in sys.path

        context.register_libs_path(tmpdir)

        # Now it should be
        assert tmpdir in sys.path

def test_register_libs_path_invalid_dir():
    with pytest.raises(ValueError):
        context.register_libs_path("/non/existing/path/to/libs")

def test_get_caller_script_fallback(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["/home/user/myscript.py"])
    caller = context.get_caller()
    assert caller == "script:myscript.py"

def test_get_caller_unknown(monkeypatch):
    def fake_get_connection_file():
        raise FileNotFoundError()

    monkeypatch.setattr(context, "get_connection_file", fake_get_connection_file)
    monkeypatch.setattr(sys, "argv", [""])  # simulate no script name

    caller = context.get_caller()
    assert caller == "unknown-session"
