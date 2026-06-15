import os
import json
import tempfile
import unittest
from unittest.mock import patch
import yaml
from plf.experiment import export_ppl_to_json, export_ppl_to_yaml
from plf.utils import Db

class TestPplExportUtilities(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for database and exports
        self.test_dir = tempfile.TemporaryDirectory()
        self.data_path = self.test_dir.name
        
        # Initialize a dummy ppls.db schema for testing
        self.db_path = os.path.join(self.data_path, "ppls.db")
        db = Db(db_path=self.db_path)
        db.execute("""
            CREATE TABLE IF NOT EXISTS ppls (
                pplid TEXT PRIMARY KEY,
                status TEXT,
                created_at TEXT,
                workflow TEXT,
                args TEXT
            )
        """)
        db.execute(
            "INSERT INTO ppls (pplid, status, created_at, workflow, args) VALUES (?, ?, ?, ?, ?)",
            ("ppl_data_run_001", "frozen", "2026-11-02T14:30:00", '{"loc": "my_workflows.GenericDataWorkflow"}', '{"param": 42}')
        )
        db.close()

    def tearDown(self):
        self.test_dir.cleanup()

    @patch('plf.experiment.get_shared_data')
    def test_export_to_json_success(self, mock_get_shared_data):
        mock_get_shared_data.return_value = {"data_path": self.data_path}
        output_path = os.path.join(self.data_path, "output.json")
        
        export_ppl_to_json("ppl_data_run_001", output_path=output_path)
        
        self.assertTrue(os.path.exists(output_path))
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["pplid"], "ppl_data_run_001")
        self.assertEqual(data["status"], "frozen")

    @patch('plf.experiment.get_shared_data')
    def test_export_to_yaml_success(self, mock_get_shared_data):
        mock_get_shared_data.return_value = {"data_path": self.data_path}
        output_path = os.path.join(self.data_path, "output.yaml")
        
        export_ppl_to_yaml("ppl_data_run_001", output_path=output_path)
        
        self.assertTrue(os.path.exists(output_path))
        with open(output_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        self.assertEqual(data["pplid"], "ppl_data_run_001")
        self.assertEqual(data["args"]["param"], 42)

    @patch('plf.experiment.get_shared_data')
    def test_export_invalid_id_raises_error(self, mock_get_shared_data):
        mock_get_shared_data.return_value = {"data_path": self.data_path}
        with self.assertRaises(ValueError):
            export_ppl_to_json("non_existent_id", output_path=os.path.join(self.data_path, "fail.json"))