
import os
import json
import tempfile
import shutil

import pytest

from lwflow.lab import create_project

@pytest.fixture
def temp_project_dir():
    """Temporary project directory fixture."""
    tmp_dir = tempfile.mkdtemp()
    yield tmp_dir
    shutil.rmtree(tmp_dir)


def test_create_project_full(temp_project_dir):
    settings = {
        "project_dir": temp_project_dir,
        "project_name": "mytestlab",
        "component_dir": os.path.join(temp_project_dir, "components")
    }

    # Run create_project
    settings_path = create_project(settings)

    # Check that returned path is correct and file exists
    assert os.path.exists(settings_path)
    assert settings_path.endswith(".json")

    # Load the saved settings
    with open(settings_path, "r", encoding="utf-8") as f:
        saved = json.load(f)

    # All expected keys should be in saved settings
    for key in ["project_dir", "project_name", "data_path", "setting_path", "component_dir"]:
        assert key in saved

    # All paths should be absolute
    for key in ["data_path", "setting_path", "component_dir"]:
        assert os.path.isabs(saved[key])

    data_path = saved["data_path"]

    # Check directories
    assert os.path.isdir(data_path)
    assert os.path.isdir(os.path.join(data_path, "Configs"))
    assert os.path.isdir(os.path.join(data_path, "Archived", "Configs"))
    assert os.path.isdir(os.path.join(data_path, "Transfer", "Configs"))
    assert os.path.isdir(saved["component_dir"])

    # Check databases
    logs_db_path = os.path.join(data_path, "logs.db")
    ppls_db_path = os.path.join(data_path, "ppls.db")
    archived_ppls_db_path = os.path.join(data_path, "Archived", "ppls.db")

    for db in [logs_db_path, ppls_db_path, archived_ppls_db_path]:
        assert os.path.exists(db)
        assert os.path.getsize(db) > 0  # Not empty

    expected_path = os.path.join(saved["data_path"], saved["project_name"] + ".json")
    assert saved["setting_path"] == expected_path
    assert settings_path == expected_path
