"""Tests for plf.experiment management utilities."""

import json
import os

import pytest

from plf.experiment import compare_ppl_configs, get_ppl_history
from plf.utils import Db, hash_args


def _register_ppl(data_path: str, pplid: str, cnfg: dict) -> None:
    """Insert a pipeline record and write its config file (no workflow needed)."""
    configs_dir = os.path.join(data_path, "Configs")
    os.makedirs(configs_dir, exist_ok=True)

    config_path = os.path.join(configs_dir, f"{pplid}.json")
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump(cnfg, handle)

    args_for_hash = {"workflow": cnfg["workflow"], "args": cnfg["args"]}
    db = Db(db_path=os.path.join(data_path, "ppls.db"))
    db.execute(
        "INSERT INTO ppls (pplid, args_hash) VALUES (?, ?)",
        (pplid, hash_args(args_for_hash)),
    )
    db.close()


def _sample_config(pplid: str, learning_rate: float) -> dict:
    """Build a minimal pipeline config for testing."""
    return {
        "pplid": pplid,
        "workflow": {"loc": "examples.DemoWorkflow", "args": {}},
        "args": {
            "data_source": {
                "loc": "examples.DataComponent",
                "args": {"learning_rate": learning_rate},
            }
        },
    }


def test_compare_ppl_configs_identical(setup_lab_env):
    """Two pipelines with the same workflow/args should compare as identical."""
    data_path = setup_lab_env["settings"]["data_path"]
    cnfg_a = _sample_config("ppl_alpha", learning_rate=0.01)
    cnfg_b = _sample_config("ppl_beta", learning_rate=0.01)

    _register_ppl(data_path, "ppl_alpha", cnfg_a)
    _register_ppl(data_path, "ppl_beta", cnfg_b)

    result = compare_ppl_configs("ppl_alpha", "ppl_beta")
    assert result["identical"] is True
    assert result["differences"] == {}


def test_compare_ppl_configs_detects_differences(setup_lab_env):
    """Changed args should appear in the differences dict."""
    data_path = setup_lab_env["settings"]["data_path"]
    _register_ppl(data_path, "ppl_low", _sample_config("ppl_low", 0.001))
    _register_ppl(data_path, "ppl_high", _sample_config("ppl_high", 0.1))

    result = compare_ppl_configs("ppl_low", "ppl_high")
    assert result["identical"] is False
    assert "args.data_source.args.learning_rate" in result["differences"]
    diff = result["differences"]["args.data_source.args.learning_rate"]
    assert diff["left"] == 0.001
    assert diff["right"] == 0.1


def test_compare_ppl_configs_missing_pipeline(setup_lab_env):
    """Requesting an unknown pipeline ID should raise ValueError."""
    data_path = setup_lab_env["settings"]["data_path"]
    _register_ppl(data_path, "ppl_exists", _sample_config("ppl_exists", 0.01))

    with pytest.raises(ValueError, match="not found"):
        compare_ppl_configs("ppl_exists", "ppl_missing")


def test_get_ppl_history_empty(setup_lab_env):
    """A pipeline that has never run should return an empty DataFrame."""
    data_path = setup_lab_env["settings"]["data_path"]
    _register_ppl(data_path, "ppl_idle", _sample_config("ppl_idle", 0.01))

    history = get_ppl_history("ppl_idle")
    assert history.empty
    assert "called_at" in history.columns


def test_get_ppl_history_with_runs(setup_lab_env):
    """Run records should be returned with session info from logs.db."""
    data_path = setup_lab_env["settings"]["data_path"]
    _register_ppl(data_path, "ppl_runner", _sample_config("ppl_runner", 0.01))

    ppls_db = Db(db_path=os.path.join(data_path, "ppls.db"))
    ppls_db.execute(
        "INSERT INTO runnings (pplid, logid, parity) VALUES (?, ?, ?)",
        ("ppl_runner", "log0", None),
    )
    ppls_db.close()

    history = get_ppl_history("ppl_runner")
    assert len(history) == 1
    assert history.iloc[0]["pplid"] == "ppl_runner"
    assert history.iloc[0]["logid"] == "log0"
    # log0 is created during lab_setup in the fixture.
    assert history.iloc[0]["called_at"] is not None


def test_get_ppl_history_missing_pipeline(setup_lab_env):
    """Unknown pipeline IDs should raise ValueError."""
    with pytest.raises(ValueError, match="not found"):
        get_ppl_history("does_not_exist")
