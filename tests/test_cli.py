import pandas as pd
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from plf.cli import main

runner = CliRunner()


def invoke(args, settings="fake_settings.json"):
    return runner.invoke(main, ["--settings", settings] + args)


@patch("plf.cli.lab_setup")
@patch("plf.cli.get_ppls", return_value=["ppl_001", "ppl_002"])
def test_list(mock_ppls, mock_setup):
    result = invoke(["list"])
    assert result.exit_code == 0
    assert "ppl_001" in result.output
    assert "ppl_002" in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.get_ppls", return_value=[])
def test_list_empty(mock_ppls, mock_setup):
    result = invoke(["list"])
    assert result.exit_code == 0
    assert "No active pipelines." in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.get_ppl_status")
def test_status(mock_status, mock_setup):
    mock_status.return_value = pd.DataFrame({
        "pplid": ["ppl_001"],
        "status": ["frozen"],
        "last_run": ["2025-11-02 14:30:00"],
        "result": [0.923],
    })
    result = invoke(["status"])
    assert result.exit_code == 0
    assert "ppl_001" in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.get_ppl_status", return_value=pd.DataFrame())
def test_status_empty(mock_status, mock_setup):
    result = invoke(["status"])
    assert result.exit_code == 0
    assert "No pipelines found." in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.PipeLine")
def test_run(mock_pipeline_cls, mock_setup):
    mock_instance = MagicMock()
    mock_pipeline_cls.return_value = mock_instance
    result = invoke(["run", "ppl_001"])
    assert result.exit_code == 0
    mock_instance.load.assert_called_once_with("ppl_001")
    mock_instance.prepare.assert_called_once()
    mock_instance.run.assert_called_once()
    assert "Finished: ppl_001" in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.archive_ppl")
def test_archive(mock_archive, mock_setup):
    result = invoke(["archive", "ppl_001"])
    assert result.exit_code == 0
    mock_archive.assert_called_once_with(["ppl_001"])
    assert "Archived: ppl_001" in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.delete_ppl")
def test_delete(mock_delete, mock_setup):
    result = invoke(["delete", "ppl_001"])
    assert result.exit_code == 0
    mock_delete.assert_called_once_with(["ppl_001"])
    assert "Deleted: ppl_001" in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.get_runnings", return_value=pd.DataFrame())
def test_stop_idle(mock_runnings, mock_setup):
    result = invoke(["stop"])
    assert result.exit_code == 0
    assert "No pipeline is currently running." in result.output


@patch("plf.cli.lab_setup")
@patch("plf.cli.PipeLine")
@patch("plf.cli.get_runnings")
def test_stop_running(mock_runnings, mock_pipeline_cls, mock_setup):
    mock_runnings.return_value = pd.DataFrame({"pplid": ["ppl_001"]})
    mock_instance = MagicMock()
    mock_pipeline_cls.return_value = mock_instance
    result = invoke(["stop"])
    assert result.exit_code == 0
    mock_instance.load.assert_called_once_with("ppl_001")
    mock_instance.stop_running.assert_called_once()


@patch("plf.cli.lab_setup")
@patch("plf.cli.PipeLine")
def test_export_json(mock_pipeline_cls, mock_setup, tmp_path):
    mock_instance = MagicMock()
    mock_instance.cnfg = {"pplid": "ppl_001", "args": {}}
    mock_pipeline_cls.return_value = mock_instance

    out_dir = tmp_path / "out"
    result = invoke(["export", "ppl_001", "--out", str(out_dir)])

    assert result.exit_code == 0
    out_file = out_dir / "ppl_001.json"
    assert out_file.exists()


@patch("plf.cli.create_project", return_value="/fake/path/settings.json")
def test_init(mock_create_project, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text('{"project_name": "test", "project_dir": ".", "component_dir": "."}')

    result = runner.invoke(main, ["init", "--config", str(config_path)])

    assert result.exit_code == 0
    mock_create_project.assert_called_once()
    assert "Lab initialized" in result.output


def test_help():
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "plf" in result.output
