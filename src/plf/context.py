import os
import sys
import inspect
import json
from ipykernel.connect import get_connection_file

class _RuntimeContext:
    """Internal class for managing shared runtime context. Do not instantiate directly."""

    _context_data = {}

    def __init__(self):
        self.context_id = self._get_context_id()
        if self.context_id not in self._context_data:
            self._context_data[self.context_id] = {}

    def _get_context_id(self) -> str:
        frame = inspect.currentframe()
        try:
            while frame.f_back:
                frame = frame.f_back
            return frame.f_code.co_filename
        finally:
            del frame

    def get_shared_data(self) -> dict:
        return self._context_data[self.context_id]

    def set_shared_data(self, data: dict, logid: str = None) -> dict:
        if isinstance(data, dict):
            if logid:
                data["logid"] = logid
            self._context_data[self.context_id] = data
        else:
            self._context_data[self.context_id] = {"logid": logid}
        return self._context_data[self.context_id]

    def get_caller(self) -> str:
        try:
            with open(get_connection_file(), encoding="utf-8") as c:
                caller = json.load(c)["jupyter_session"]
        except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError, RuntimeError):
            if len(sys.argv) > 0 and sys.argv[0]:
                caller = f"script:{os.path.basename(sys.argv[0])}"
            else:
                caller = "unknown-session"
        return caller

    def register_libs_path(self, libs_dir: str) -> None:
        libs_path = os.path.abspath(libs_dir)
        if not os.path.isdir(libs_dir):
            raise ValueError(f"Invalid directory: {libs_dir}")
        if libs_path not in sys.path:
            sys.path.append(libs_path)


# Singleton instance — only this should be used outside the module
context = _RuntimeContext()
