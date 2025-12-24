
from pathlib import Path
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd
import uuid
from copy import deepcopy



from .context import set_shared_data, get_caller, register_libs_path, get_shared_data
from .utils import Db


class Lab:
    def __init__(self, settings: Optional[dict] = None, settings_path: Optional[str] = None):
        """
        Initialize the LabManager with the provided settings or by loading from a settings file.
        """
        if settings:
            self.settings = settings
        elif settings_path:
            self.load_settings(settings_path)
        else:
            # 🔥 THIS IS THE IMPORTANT PART
            ctx = get_shared_data()
            if not ctx:
                raise ValueError(
                    "No active lab found in context. "
                    "Provide `settings` or `settings_path`, or call lab_setup() first."
                )
            self.settings = ctx

        self.project_dir = os.path.abspath(self.settings["project_dir"])
        self.project_name = self.settings["project_name"]
        self.component_dir = os.path.abspath(self.settings["component_dir"])
        self.data_path = os.path.join(self.project_dir, self.project_name)

    def load_settings(self, settings_path: str) -> None:
        """
        Load lab settings from the specified file.
        """
        if os.path.exists(settings_path):
            with open(settings_path, encoding="utf-8") as f:
                self.settings = json.load(f)
        else:
            raise FileNotFoundError(f"Settings file not found at {settings_path}")

    def export_settings(self) -> str:
        """
        Export the current settings to a JSON file.
        """
        pth = os.path.join(Path(self.settings['data_path']).parent, self.settings["project_name"] + ".json")
        with open(pth, "w", encoding="utf-8") as out_file:
            json.dump(self.settings, out_file, indent=4)
        return pth

    def create_project(self) -> str:
        """
        Create the project directory structure, databases, and settings file.
        """
        project_name = self.settings["project_name"]
        component_dir = self.settings["component_dir"]

        # Derived paths
        data_path = os.path.join(self.project_dir, project_name)
        setting_path = os.path.join(data_path, f"{project_name}.json")

        # Update settings with absolute paths
        self.settings.update({
            "project_dir": self.project_dir,
            "component_dir": component_dir,
            "data_path": data_path,
            "setting_path": setting_path,
        })

        # Create required directories
        os.makedirs(self.settings["data_path"], exist_ok=True)
        os.makedirs(self.settings["component_dir"], exist_ok=True)

        # Remove old databases if any
        for db_file in ["logs.db", "ppls.db"]:
            db_path = os.path.join(data_path, db_file)
            if os.path.exists(db_path):
                os.remove(db_path)

        # Setup DBs and shared data
        self.setup_databases()
        set_shared_data(self.settings)

        # Save settings file
        with open(setting_path, "w", encoding="utf-8") as f:
            json.dump(self.settings, f, indent=4)

        return setting_path

    def setup_databases(self) -> None:
        """
        Sets up the required databases for the lab project, including:
        - logs.db (with logs table)
        - ppls.db (with ppls, edges, runnings tables)
        - Archived/ppls.db (with ppls table)
        """
        self._create_and_init_db(
            os.path.join(self.settings["data_path"], "logs.db"),
            ["CREATE TABLE IF NOT EXISTS logs (logid TEXT PRIMARY KEY, called_at TEXT NOT NULL, created_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);"],
            [("INSERT INTO logs (logid, called_at) VALUES (?, ?)", ('log0', get_caller()))]
        )

        self._create_and_init_db(
            os.path.join(self.settings["data_path"], "ppls.db"),
            [
                """
                CREATE TABLE IF NOT EXISTS ppls (
                    pplid TEXT PRIMARY KEY,
                    args_hash TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'init'
                        CHECK(status IN ('init', 'running', 'frozen', 'cleaned')),
                    created_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS edges (
                    edgid INTEGER PRIMARY KEY AUTOINCREMENT,
                    prev TEXT NOT NULL,
                    next TEXT NOT NULL,
                    desc TEXT,
                    directed BOOL DEFAULT TRUE,
                    FOREIGN KEY(prev) REFERENCES ppls(pplid),
                    FOREIGN KEY(next) REFERENCES ppls(pplid)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS runnings (
                    runid INTEGER PRIMARY KEY AUTOINCREMENT,
                    pplid NOT NULL,
                    logid TEXT DEFAULT NULL,
                    parity TEXT DEFAULT NULL,
                    started_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(pplid) REFERENCES ppls(pplid)
                );
                """
            ]
        )

        os.makedirs(os.path.join(self.settings["data_path"], "Archived"), exist_ok=True)

        self._create_and_init_db(
            os.path.join(self.settings["data_path"], "Archived", "ppls.db"),
            [
                """
                CREATE TABLE IF NOT EXISTS ppls (
                    pplid TEXT PRIMARY KEY,
                    args_hash TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'init'
                        CHECK(status IN ('init', 'running', 'frozen', 'cleaned')),
                    created_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            ]
        )

    def _create_and_init_db(self, db_path: str, tables: List[str], init_statements: List[tuple] = None) -> None:
        """
        Helper method to create and initialize databases.
        """
        db = Db(db_path=db_path)
        for table_sql in tables:
            db.execute(table_sql)
        if init_statements:
            for stmt, params in init_statements:
                db.execute(stmt, params)
        db.close()

    def lab_setup(self) -> None:
        """
        Set up the lab environment by initializing log and shared data.
        """
        caller = get_caller()
        log_path = os.path.join(self.settings["data_path"], "logs.db")
        db = Db(db_path=log_path)

        # Get current number of logs
        cursor = db.execute("SELECT COUNT(*) FROM logs")
        row_count = cursor.fetchone()[0]
        logid = f"log{row_count}"
        db.execute(
            "INSERT INTO logs (logid, called_at) VALUES (?, ?)",
            (logid,  caller)
        )

        db.close()
        set_shared_data(self.settings, logid)
        register_libs_path(self.settings["component_dir"])

    def get_logs(self) -> pd.DataFrame:
        """
        Fetch all logs as a pandas DataFrame.
        """
        log_path = os.path.join(self.settings["data_path"], "logs.db")
        db = Db(db_path=log_path)
        cursor = db.execute("SELECT * FROM logs")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        db.close()
        return pd.DataFrame(rows, columns=col_names)

    def clone_lab(self, name: str, description: str = "", clone_type: str = "remote") -> str:
        """
        Create a new lab clone with isolated settings and directories.

        Args:
            name: Human-readable name for the clone.
            description: Optional description for the clone.
            clone_type: Type of clone (e.g., 'remote', 'local').

        Returns:
            str: Unique clone ID.
        """
        clone_id = str(uuid.uuid4())
        clone_settings = deepcopy(self.settings)  # isolate settings

        base_path = self.settings["data_path"]
        clone_base_path = os.path.join(
            self.settings.get("project_dir", base_path),
            f"{self.project_name}_clone_{clone_id[:8]}"
        )
        os.makedirs(clone_base_path, exist_ok=True)

        # Initialize path and location maps
        path_map = {base_path: clone_base_path}
        loc_map = {base_path: clone_base_path}  # default, can be customized per experiment

        clone_dict = {
            "name": name,
            "clone_id": clone_id,
            "description": description,
            "type": clone_type,
            "settings": {
                "base_path": clone_base_path,
                "path_map": path_map,
                "loc_map": loc_map,
                "original_settings": clone_settings
            },
            "created_time": datetime.now().isoformat(),
            "transfers": []  # initialize empty transfer list
        }

        # Add clone to settings
        self.settings.setdefault("clones", []).append(clone_dict)
        self.export_settings()

        print(f"[INFO] Created lab clone '{name}' with id {clone_id} at {clone_base_path}")
        return clone_id

    def transfer_experiment(self, ppl: Dict[str, Any], clone_id: str) -> str:
        """
        Transfer an experiment to a specific clone. Only metadata and paths are mapped initially.

        Args:
            ppl: Dictionary representing the experiment (e.g., configuration, pplid).
            clone_id: Target clone ID.

        Returns:
            str: Unique transfer ID for this experiment transfer.
        """
        # Find clone
        clones = self.settings.get("clones", [])
        clone = next((c for c in clones if c["clone_id"] == clone_id), None)
        if not clone:
            raise ValueError(f"No clone found with id {clone_id}")

        transfer_id = str(uuid.uuid4())

        # Extract locations/paths from experiment
        from .utils import extract_locs  # existing helper
        locs = extract_locs(ppl)

        base_path = self.settings["data_path"]
        clone_base_path = clone["settings"]["base_path"]

        # Default path and location mapping (identity)
        path_map = {base_path: clone_base_path}
        loc_map = {loc: loc for loc in locs}

        transfer_dict = {
            "transfer_id": transfer_id,
            "pplid": ppl["pplid"],
            "clone_id": clone_id,
            "paths": path_map,
            "locs": loc_map,
            "created_time": datetime.now().isoformat()
        }

        # Store transfer in clone metadata
        clone.setdefault("transfers", []).append(transfer_dict)
        self.export_settings()

        print(f"[INFO] Experiment '{ppl['pplid']}' transferred to clone {clone_id} with transfer_id {transfer_id}")
        return transfer_id
