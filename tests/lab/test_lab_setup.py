import os
import sqlite3
import pytest
import tempfile


from lwflow.lab import lab_setup, setup_databases  # Adjust import to your structure

@pytest.fixture
def temp_lab_setup():
    """Fixture to create a temp lab environment and clean up after."""
    temp_dir = tempfile.mkdtemp()
    project_name = "mytestlab"
    data_path = os.path.join(temp_dir, project_name)
    component_dir = os.path.join(data_path, "components")
    os.makedirs(component_dir, exist_ok=True)

    # Create missing directories required by setup_databases
    os.makedirs(os.path.join(data_path, "Archived"), exist_ok=True)
    os.makedirs(os.path.join(data_path, "Transfer"), exist_ok=True)

    settings = {
        "project_name": project_name,
        "project_dir": temp_dir,
        "data_path": data_path,
        "component_dir": component_dir,
    }

    # Now call setup_databases
    setup_databases(settings)
    # Save settings to JSON file
    settings_path = os.path.join(data_path, f"{project_name}.json")
    with open(settings_path, "w", encoding="utf-8") as f:
        import json
        json.dump(settings, f, indent=4)

    yield settings_path, settings

    # Cleanup after test
    import shutil
    shutil.rmtree(temp_dir)


def test_lab_setup_inserts_log_and_sets_shared_data(temp_lab_setup):
    settings_path, settings = temp_lab_setup

    # Connect to logs.db before lab_setup to get initial count
    logs_db_path = os.path.join(settings["data_path"], "logs.db")
    conn = sqlite3.connect(logs_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM logs")
    initial_count = cursor.fetchone()[0]
    conn.close()

    # Run lab_setup (this should insert one more log)
    lab_setup(settings_path)

    # Reconnect to check log count incremented by 1
    conn = sqlite3.connect(logs_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM logs")
    new_count = cursor.fetchone()[0]
    assert new_count == initial_count + 1, f"Expected log count to increase by 1, got {new_count}"

    # Check last inserted logid is log{new_count-1} (assuming 0-based numbering)
    cursor.execute("SELECT logid FROM logs ORDER BY rowid DESC LIMIT 1")
    last_logid = cursor.fetchone()[0]
    conn.close()

    expected_logid = f"log{new_count - 1}"
    assert last_logid == expected_logid, f"Expected last logid to be {expected_logid}, got {last_logid}"
