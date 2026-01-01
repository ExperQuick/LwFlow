# transfer.py
import json
import zipfile
from pathlib import Path
from datetime import datetime
from uuid import uuid4
import hashlib
import shutil
from copy import deepcopy

from .utils import Db
from .context import get_shared_data, set_shared_data
from ._pipeline import PipeLine

# ---------------------------
# Role enforcement
# ---------------------------
def _ensure_base(settings):
    if settings.get("lab_role") != "base":
        raise RuntimeError("Operation allowed only in BASE lab")

def _ensure_remote(settings):
    if settings.get("lab_role") != "remote":
        raise RuntimeError("Operation allowed only in REMOTE lab")

# ---------------------------
# TransferContext
# ---------------------------
class TransferContext:
    """Runtime context for remapping paths and components on remote."""
    def __init__(self, path_map=None, component_map=None, transfer_id=None):
        self.path_map = {Path(k).as_posix(): Path(v).as_posix() for k, v in (path_map or {}).items()}
        self.component_map = component_map or {}
        self.transfer_id = transfer_id

    def map_path(self, path: str) -> str:
        path = Path(path).as_posix()
        for src, dst in self.path_map.items():
            if path.startswith(src):
                return path.replace(src, dst, 1)
        return path

    def map_component(self, loc: str) -> str:
        return self.component_map.get(loc, loc)

# ---------------------------
# Safe ZIP extraction
# ---------------------------
def _safe_extract(zip_path: Path, target_dir: Path):
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            p = Path(member.filename)
            if p.is_absolute() or ".." in p.parts:
                raise ValueError("Unsafe ZIP content detected")
        zf.extractall(target_dir)

# ---------------------------
# Extract paths and locs from config
# ---------------------------
def extract_paths_and_locs(config: dict):
    paths = set()
    locs = set()

    def recurse(d):
        if isinstance(d, dict):
            for k, v in d.items():
                if k == "loc":
                    locs.add(v)
                elif k in ("src", "path", "data_path"):
                    paths.add(v)
                else:
                    recurse(v)
        elif isinstance(d, list):
            for item in d:
                recurse(item)
    recurse(config)
    return paths, locs

# ---------------------------
# Export Transfer
# ---------------------------
def export_transfer(ppls, clone_id=None, transfer_type="run", prev_transfer_id=None, mode="copy"):
    """
    Export pipelines from current lab.

    BASE lab: full export
    REMOTE lab: results-only export
    """
    settings = get_shared_data()
    role = settings.get("lab_role")

    if role == "base":
        if not clone_id:
            raise ValueError("clone_id required for base export")
        return _export_base_to_remote(ppls, clone_id, transfer_type, mode)

    if role == "remote":
        if transfer_type != "results":
            raise RuntimeError("Remote can only export results back to base")
        return _export_remote_to_base(ppls, prev_transfer_id, mode)

    raise RuntimeError("Unknown lab role")

# ---------------------------
# Import Transfer
# ---------------------------
def import_transfer(zip_path: Path):
    """Import a transfer ZIP into current lab"""
    settings = get_shared_data()

    with zipfile.ZipFile(zip_path) as zf:
        meta = json.loads(zf.read("transfer.json"))

    direction = meta.get("direction")

    if direction == "base_to_remote":
        _ensure_remote(settings)
        return _import_on_remote(zip_path, meta)

    if direction == "remote_to_base":
        _ensure_base(settings)
        return _import_on_base(zip_path, meta)

    raise RuntimeError("Unknown transfer direction")


def collect_transfer_meta(ppls):
    all_paths = set()
    all_locs = set()

    for pplid in ppls:
        P = PipeLine(pplid)
        P.load(pplid)

        cfg_path = Path(P.get_path(of="config"))
        config = json.loads(cfg_path.read_text())

        paths, locs = extract_paths_and_locs(config)
        all_paths.update(paths)
        all_locs.update(locs)

    return {
        "srcs": sorted(all_paths),
        "locs": sorted(all_locs),
    }


def _payload_name(src: str) -> str:
    h = hashlib.sha1(src.encode()).hexdigest()[:8]
    return f"p_{h}"


def _write_payload(zf, lab_base: Path, paths: set):
    path_map = {}

    for src in sorted(paths):
        src_path = Path(src).resolve()
        payload_name = _payload_name(src)
        zip_root = Path("payload") / payload_name

        path_map[src] = zip_root.as_posix()

        if not src_path.exists():
            continue

        if src_path.is_dir():
            for f in src_path.rglob("*"):
                if f.is_file():
                    zf.write(
                        f,
                        zip_root / f.relative_to(src_path)
                    )
        else:
            zf.write(src_path, zip_root / src_path.name)

    return path_map

# ---------------------------
# Internal: BASE -> REMOTE
# ---------------------------
def _export_base_to_remote(ppls, clone_id, transfer_type, mode="copy"):
    settings = get_shared_data()
    _ensure_base(settings)

    lab_base = Path(settings["data_path"]).resolve()
    clone_dir = lab_base / "Clones" / clone_id
    if not clone_dir.exists():
        raise ValueError(f"Clone '{clone_id}' not registered")

    transfer_id = f"t_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    zip_path = clone_dir / "transfers" / f"{transfer_id}.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    paths, locs = collect_transfer_meta(ppls)
    

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        path_map = _write_payload(zf, lab_base, paths)
        # ---- LOCS: single .py file ----
        loc_map = _write_loc_payload(zf, lab_base, locs, transfer_id)

        transfer_meta = {
            "transfer_id": transfer_id,
            "origin_lab_id": settings.get("lab_id"),
            "target_lab_id": clone_id,
            "direction": "base_to_remote",
            "transfer_type": transfer_type,
            "created_at": datetime.utcnow().isoformat(),
            "ppls": _collect_ppls_meta(ppls),
            "transfer_meta": {
                "path_map": path_map,
                "loc_map": loc_map,
            }
        }
        for pplid in ppls:
            P = PipeLine()
            if not P.verify(pplid=pplid):
                raise ValueError(f"Invalid pplid: {pplid}")

            P.load(pplid)

            # ---- CONFIG (always file) ----
            cfg = Path(P.get_path(of="config")).resolve()
            arcname = cfg.relative_to(lab_base)
            zf.write(cfg, arcname)

            # ---- ARTIFACTS (file or dir) ----
            for art in P.paths:
                if art == "config":
                    continue

                try:
                    p = Path(P.get_path(of=art)).resolve()
                    if not p.exists():
                        continue

                    if p.is_dir():
                        for f in p.rglob("*"):
                            if f.is_file():
                                zf.write(
                                    f,
                                    f.relative_to(lab_base)
                                )
                    else:
                        zf.write(
                            p,
                            p.relative_to(lab_base)
                        )
                except Exception:
                    pass

        # ---- transfer.json LAST ----
        zf.writestr("transfer.json", json.dumps(transfer_meta, indent=4))

    # ---- REGISTER TRANSFER ----
    clone_json = clone_dir / "clone.json"
    clone_cfg = json.loads(clone_json.read_text())
    clone_cfg.setdefault("transfers", []).append(transfer_id)
    clone_json.write_text(json.dumps(clone_cfg, indent=4))

    # ---- MOVE MODE ----
    if mode == "move":
        for pplid in ppls:
            P = PipeLine(pplid)
            for art in P.paths:
                if art == "config":
                    continue
                p = Path(P.get_path(of=art))
                if p.exists():
                    if p.is_dir():
                        shutil.rmtree(p)
                    else:
                        p.unlink()

    return zip_path

# ---------------------------
# Internal: REMOTE -> BASE (results only)
# ---------------------------
def _export_remote_to_base(ppls, prev_transfer_id=None, mode="copy"):
    settings = get_shared_data()
    _ensure_remote(settings)

    transfer_id = f"r_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    zip_path = Path(settings["data_path"]) / "TransfersOut" / f"{transfer_id}.zip"
    zip_path.parent.mkdir(exist_ok=True)

    meta = {
        "transfer_id": transfer_id,
        "origin_lab_id": settings["lab_id"],
        "prev_transfer_id": prev_transfer_id,
        "direction": "remote_to_base",
        "transfer_type": "results",
        "created_at": datetime.utcnow().isoformat(),
        "ppls": ppls
    }

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("transfer.json", json.dumps(meta, indent=4))

        for pplid in ppls:
            P = PipeLine(pplid)
            for art in list(P.paths):
                if art == "config":
                    continue
                p = Path(P.get_path(of=art))
                if not p.exists():
                    continue
                if p.is_dir():
                    for f in p.rglob("*"):
                        if f.is_file():
                            zf.write(f, f"Results/{pplid}/{art}/{f.relative_to(p)}")
                else:
                    zf.write(p, f"Results/{pplid}/{art}/{p.name}")

    # Move mode
    if mode == "move":
        for pplid in ppls:
            P = PipeLine(pplid)
            for art in list(P.paths):
                if art == "config":
                    continue
                p = Path(P.get_path(of=art))
                if p.exists():
                    if p.is_dir():
                        shutil.rmtree(p)
                    else:
                        p.unlink()

    return zip_path

# ---------------------------
# Internal: import on remote
# ---------------------------
def _import_on_remote(zip_path, meta, mode="copy"):
    settings = get_shared_data()
    lab_base = Path(settings["data_path"]).resolve()

    transfers_dir = lab_base / "Transfers"
    transfers_dir.mkdir(exist_ok=True)

    # ---- Stage extraction ----
    extract_dir = transfers_dir / meta["transfer_id"]
    extract_dir.mkdir(parents=True, exist_ok=True)

    _safe_extract(zip_path, extract_dir)

    # ---- Materialize into lab ----
    for item in extract_dir.rglob("*"):
        if item.is_dir():
            continue

        # transfer.json stays in Transfers
        if item.name == "transfer.json":
            continue

        rel_path = item.relative_to(extract_dir)
        target = lab_base / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            target.unlink()

        if mode == "move":
            shutil.move(item, target)
        else:
            shutil.copy2(item, target)


    # ---- Register pipelines ----
    _register_ppls_in_db(meta.get("ppls", {}))

    # ---- Attach transfer context (provenance only) ----
    tm = meta.get("transfer_meta", {})

    ctx = TransferContext(
        path_map={
            src: str(lab_base / dst)
            for src, dst in tm.get("path_map", {}).items()
        },
        component_map={},   # future
        transfer_id=meta["transfer_id"],
    )


    settings["transfer_context"] = ctx
    set_shared_data(settings)

    return True
# ---------------------------
# Helper: write single .py file for locs
# ---------------------------
def _loc_hash(code: str) -> str:
    return hashlib.sha1(code.encode()).hexdigest()[:8]

def _write_loc_payload(zf, lab_base: Path, locs: set, transfer_id: str):
    """
    Collect all component classes defined in locs and their imports,
    write them into a single <transfer_id>.py file inside the zip,
    and return mapping of full loc -> transfer_id.classhash
    """
    loc_map = {}
    code_chunks = []
    processed_modules = set()
    settings = get_shared_data()
    component_dir = Path(settings["component_dir"]).resolve()
    for loc in sorted(locs):
        if '.' not in loc:
            continue
        module_name, class_name = loc.rsplit('.', 1)

        module_path = component_dir / f"{module_name}.py"
        if not module_path.exists():
            print(f"Warning: component file not found: {module_path}")
            continue

        # Skip module if already processed
        if module_path in processed_modules:
            continue
        processed_modules.add(module_path)

        code_text = module_path.read_text(encoding='utf-8')

        # Extract imports (lines starting with import or from)
        imports = []
        classes = {}
        for line in code_text.splitlines():
            stripped = line.strip()
            if stripped.startswith("import ") or stripped.startswith("from "):
                imports.append(line)

        # Extract class definitions (simple regex for top-level classes)
        import re
        class_pattern = re.compile(r"^class\s+(\w+)[\(:]", re.MULTILINE)
        for match in class_pattern.finditer(code_text):
            cls_name = match.group(1)
            # get class source by splitting lines
            lines = code_text.splitlines()
            start = match.start()
            # crude: take all lines from match to end or next class
            next_class_match = class_pattern.search(code_text, pos=match.end())
            end = next_class_match.start() if next_class_match else len(code_text)
            class_code = code_text[match.start():end].strip()
            classes[cls_name] = class_code

        # pick only classes referenced in locs from this module
        if class_name not in classes:
            print(f"Warning: class {class_name} not found in {module_path}")
            continue

        # include imports + class code
        final_code = '\n'.join(imports + [''] + [classes[class_name]])
        class_hash = hashlib.sha1(final_code.encode()).hexdigest()[:8]
        loc_map[loc] = f"{transfer_id}.{class_hash}"
        code_chunks.append(f"# --- {loc} ---\n{final_code}\n")

    # Write single .py inside zip
    py_name = f"{transfer_id}.py"
    zf.writestr(py_name, '\n\n'.join(code_chunks))
    return loc_map


# ---------------------------
# Internal: import on base
# ---------------------------
def _import_on_base(zip_path, meta):
    settings = get_shared_data()
    results_dir = Path(settings["data_path"]) / "RemoteResults" / meta["transfer_id"]
    _safe_extract(zip_path, results_dir)


def _register_ppls_in_db(ppls_meta: dict):
    settings = get_shared_data()
    db = Db(db_path=str(Path(settings["data_path"]) / "ppls.db"))

    existing = {
        row[0] for row in db.query("SELECT pplid FROM ppls")
    }

    for pplid, meta in ppls_meta.items():
        if pplid in existing:
            continue

        db.execute(
            """
            INSERT INTO ppls (pplid, args_hash, status, created_time)
            VALUES (?, ?, ?, ?)
            """,
            (
                meta["pplid"],
                meta.get("args_hash"),
                meta.get("status", "init"),
                meta.get("created_time"),
            )
        )

    db.close()

def _collect_ppls_meta(ppls):
    settings = get_shared_data()
    db = Db(db_path=str(Path(settings["data_path"]) / "ppls.db"))

    rows = db.query(
        """
        SELECT pplid, args_hash, status, created_time
        FROM ppls
        WHERE pplid IN ({})
        """.format(",".join("?" * len(ppls))),
        tuple(ppls)
    )

    db.close()

    meta = {}
    for pplid, args_hash, status, created_time in rows:
        meta[pplid] = {
            "pplid": pplid,
            "args_hash": args_hash,
            "status": status,
            "created_time": created_time
        }

    return meta


import json
from pathlib import Path
import pandas as pd

def get_clones():
    """
    Return a DataFrame of all registered clones.

    Only valid in BASE lab.
    """
    settings = get_shared_data()

    if settings.get("lab_role") != "base":
        raise RuntimeError("get_clones() is allowed only in BASE lab")

    clones_root = Path(settings["data_path"]) / "Clones"
    rows = []

    if not clones_root.exists():
        return pd.DataFrame(
            columns=[
                "clone_id",
                "clone_type",
                "name",
                "desc",
                "created_at",
                "num_transfers",
            ]
        )

    for clone_dir in clones_root.iterdir():
        if not clone_dir.is_dir():
            continue

        cfg_path = clone_dir / "clone.json"
        if not cfg_path.exists():
            continue

        try:
            with open(cfg_path, encoding="utf-8") as f:
                cfg = json.load(f)

            rows.append({
                "clone_id": cfg.get("clone_id"),
                "clone_type": cfg.get("clone_type"),
                "name": cfg.get("name"),
                "desc": cfg.get("desc"),
                "created_at": cfg.get("created_at"),
                "num_transfers": len(cfg.get("transfers", [])),
            })

        except Exception:
            # Skip broken clone entries safely
            continue

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.sort_values("created_at").reset_index(drop=True)

    return df
