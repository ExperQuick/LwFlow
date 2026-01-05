import json
from pathlib import Path



def _load_transfer_config(transfers_dir: Path):
    cfg_path = transfers_dir / "transfer_config.json"
    if not cfg_path.exists():
        return {
            "active_transfer_id": None,
            "history": [],
            "ppl_to_transfer": {}
        }
    return json.loads(cfg_path.read_text(encoding="utf-8"))


# ------


# ---------------------------
# TransferContext
# ---------------------------
class TransferContext:
    """Runtime context for remapping paths and components on remote."""

    def __init__(self, transfers_dir: Path):
        self.transfers_dir = transfers_dir
        self._cfg = _load_transfer_config(transfers_dir)

    def _load_transfer_meta(self, transfer_id: str) -> dict:
        meta_path = self.transfers_dir / transfer_id / "transfer.json"
        if not meta_path.exists():
            return {}
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def map_path(self, path: str, pplid: str) -> str:
        path = Path(path).as_posix()
        transfer_id = self._cfg["ppl_to_transfer"].get(pplid)
        if not transfer_id:
            return path

        meta = self._load_transfer_meta(transfer_id)
        path_map = meta.get("path_map", {})

        for src, dst in path_map.items():
            if path.startswith(src):
                return path.replace(src, dst, 1)

        return path

    def map_component(self, loc: str, pplid: str) -> str:
        transfer_id = self._cfg["ppl_to_transfer"].get(pplid)
        if not transfer_id:
            return loc

        meta = self._load_transfer_meta(transfer_id)
        loc_map = meta.get("loc_map", {})

        return loc_map.get(loc, loc)
