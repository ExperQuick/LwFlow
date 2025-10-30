"""
This module provides
"""

import sys
import os
from typing import overload, List, Callable, Dict, Any, Union, Optional, Literal
import json
import hashlib
import warnings
import importlib
import sqlite3
import traceback
from abc import ABC, abstractmethod

from copy import deepcopy

import pandas as pd

__all__ = [
    "load_component",
    "hash_args",
    "Component",
    "WorkFlow",
    "Db",
    "extract_all_locs",
    "get_invalid_loc_queries",
    'filter_configs',
    'get_matching'
]


class ComponentLoadError(Exception):
    """
    ComponentLoadError
    """


def load_component(
    loc: str, args: Optional[Dict[str, Any]] = None, setup: bool = True
) -> Callable:
    """
        Dynamically load and optionally initialize a component class.

        This utility imports a class from a given module path and instantiates it.
        If the class defines a `setup` method and `setup=True`, it calls `setup(args)`
        and returns the initialized component. Otherwise, it returns the raw instance.

        Parameters
        ----------
        loc : str
            Fully qualified class location in dot notation (e.g., 'CompBase.models.MyModel').
            If no dot is present, it is assumed the class is defined in `__main__`.

        args : dict, optional
            Dictionary of arguments to pass to the `setup()` method, if applicable.
            Defaults to an empty dict.

        setup : bool, optional
            Whether to invoke the component’s `setup` method after instantiation. Defaults to True.

        Returns
        -------
        Any
            An instance of the loaded class, either raw or configured via `setup()`.

        Raises
        ------
        ComponentLoadError
            If the specified class is not found in the target module.
        ImportError
            If the module cannot be imported.
    """
    args = args or {}

    # Parse module and class name
    if "." in loc:
        module_path, class_name = loc.rsplit(".", 1)
        if module_path in sys.modules:
            module = sys.modules[module_path]
            if getattr(module, "__spec__", None):
                module = importlib.reload(module)
        else:
            module = importlib.import_module(module_path)
    else:
        # No dot means class is in __main__
        module = sys.modules["__main__"]
        class_name = loc
        warnings.warn(
            f"{loc} component is not saved. "
            "Make sure to save it in an appropriate location before"
            "initiating an experiment, test, or report."
        )
    if not hasattr(module, class_name):
        raise ComponentLoadError(
            f"Class '{class_name}' not found in module '{module.__name__}'"
        )
    component_cls = getattr(module, class_name)
    component = component_cls()

    # If setup method exists and setup flag is True, call it with args
    if setup and hasattr(component, "_setup"):
        return component.setup(args)
    return component


class Component(ABC):
    """
        Base class for all components with dynamic loading capability.

        Attributes:
            loc (str): Location identifier for the component.
            args (dict): Expected keys for arguments.
    """

    def __init__(self, loc: str = None):
        self.loc = self.__class__.__name__ if loc is None else loc
        self.args = {}
        self.P = None
    
    def load_component(self, loc: str, args: Optional[Dict[str, Any]] = None, setup: bool = True):
        comp =  load_component(loc=loc, args=args,setup=setup)
        comp.P = self.P
        return comp

    def check_args(self, args: dict) -> bool:
        """Check whether provided args contain all required keys."""
        return all(arg in args for arg in self.args)

    def setup(self, args: Dict[str, Any]) -> Optional[Any]:
        """
            Set up the component with provided arguments.

            Args:
                args: Dictionary of arguments to initialize the component.

            Returns:
                Optional[Any]: Initialized component or setup result.
        """
        if self.check_args(args):
            # print(154,args)
            try:
                self._setup(args)
                return self
            except AttributeError as exc:
                raise AttributeError(
                    f"Component '{self.loc}' does not implement '_setup'"
                ) from exc
        # raise ValueError(f"Arguments {args} are incompatible with '{self.loc}'")
        traceback.print_exc()
    
    @abstractmethod
    def _setup(self, args: Dict[str, Any],P=None) -> Optional[Any]:
        """
            Private setup to be overridden in subclasses.

            Args:
                args: Dictionary of arguments.

            Returns:
                Optional[Any]
        """
        raise NotImplementedError(f"Component '{self.loc}' must implement '_setup'")

class WorkFlow(Component, ABC):
    
    def __init__(self, loc = None):
        super().__init__(loc)
        self.paths = []
    
    @abstractmethod
    def run(self):
        raise NotImplementedError(f"Workflow '{self.loc}' must implement 'run'")

    @abstractmethod
    def prepare(self):
        raise NotImplementedError(f"Workflow '{self.loc}' must implement 'prepare'")

    @abstractmethod
    def new(self):
        raise NotImplementedError(f"Workflow '{self.loc}' must implement 'new'")
    
    @abstractmethod
    def get_path(self,
        of: str,
        pplid: Optional[str] = None,
        args: Optional[Dict] = None):
        raise NotImplementedError(f"Workflow '{self.loc}' must implement 'get_path'")

    @abstractmethod
    def clean(self):
        raise NotImplementedError(f"Workflow '{self.loc}' must implement 'clean'")
    
    @abstractmethod
    def status(self):
        return {}

class Db:
    """
        Lightweight SQLite wrapper with foreign key enforcement.

        Args:
            db_path (str): Path to the SQLite database file.

        Raises:
            FileNotFoundError: If the directory for the DB path doesn't exist.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

        # Check if directory exists
        dir_name = os.path.dirname(db_path)
        if dir_name and not os.path.isdir(dir_name):
            raise FileNotFoundError(f"Directory does not exist: {dir_name}")

        self._connect()

    def _connect(self) -> None:
        """Establishes a database connection."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")

    def execute(self, query: str, params: tuple = ()) -> Optional[sqlite3.Cursor]:
        """
        Execute a SQL query (INSERT, UPDATE, DELETE).

        Args:
            query (str): SQL query string.
            params (tuple): Query parameters.

        Returns:
            sqlite3.Cursor or None
        """
        if not self.conn:
            raise ConnectionError("No database connection.")
        try:
            cur = self.conn.cursor()
            cur.execute(query, params)
            self.conn.commit()
            return cur
        except sqlite3.Error as e:
            print(f"[SQLITE ERROR] {e}")
            return None

    def query(self, query: str, params: tuple = ()) -> list:
        """
        Execute a SELECT query and fetch all results.

        Args:
            query (str): SQL query string.
            params (tuple): Query parameters.

        Returns:
            list: Query results.
        """
        cursor = self.execute(query, params)
        return cursor.fetchall() if cursor else []

    def close(self) -> None:
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def hash_args(args: Dict[str, Any]) -> str:
    """
        Generate a SHA-256 hash from a dictionary of arguments.

        This is commonly used to uniquely identify a configuration or set of parameters.

        Parameters
        ----------
        args : dict
            The dictionary of arguments to be hashed. Must be JSON-serializable.

        Returns
        -------
        str
            A SHA-256 hash string representing the input dictionary.

        Raises
        ------
        TypeError
        If the dictionary contains non-serializable values.
    """
    dict_str = json.dumps(args, sort_keys=True, separators=(",", ":"))
    # print(dict_str)
    hhas  = hashlib.sha256(dict_str.encode()).hexdigest()
    # print(hhas)
    return hhas

from typing import Union, Dict, List, Any

def extract_all_locs(d: Union[Dict, List]) -> List[str]:
    """
    Recursively extract all 'loc' values from nested dictionaries or lists.
    A component is defined as a dict with a 'loc' key and optional 'args'.
    """
    locs = []

    if isinstance(d, dict):
        # If this is a component dict
        if "loc" in d:
            locs.append(d["loc"])
            if "args" in d and isinstance(d["args"], (dict, list)):
                locs.extend(extract_all_locs(d["args"]))
        else:
            # Otherwise, check all values in the dict
            for v in d.values():
                locs.extend(extract_all_locs(v))

    elif isinstance(d, list):
        for item in d:
            locs.extend(extract_all_locs(item))

    return locs

from typing import Union, Dict, List

def get_invalid_loc_queries(d: Union[Dict, List], parent_key: str = "") -> List[str]:
    queries = []

    if isinstance(d, dict):
        # Check this dict itself
        if "loc" in d:
            loc_val = d["loc"]
            if not isinstance(loc_val, str) or "." not in loc_val:
                queries.append(parent_key)

        # Now recurse into its values
        for k, v in d.items():
            new_key = f"{parent_key}>{k}" if parent_key else k
            queries.extend(get_invalid_loc_queries(v, new_key))

    elif isinstance(d, list):
        for idx, item in enumerate(d):
            item_key = f"{parent_key}[{idx}]" if parent_key else f"[{idx}]"
            queries.extend(get_invalid_loc_queries(item, item_key))

    return queries

def _flatten_nested_locs(data: Dict[str, Any]) -> Dict[str, Any]:
    for exp in data:
        if isinstance(data[exp], dict):
            for key in list(data[exp].keys()):
                if isinstance(data[exp][key], dict):
                    data[exp][key] = data[exp][key].get("loc")
    return data


def _apply_key_filter(data: Dict[str, Any], q: str) -> Dict[str, Any]:
    for exp in list(data.keys()):
        val = data[exp].get(q)
        if isinstance(val, dict):
            data[exp] = val.get("loc")
        elif isinstance(val, (int, str)):
            data[exp] = val
        else:
            data.pop(exp)
    return data


def _apply_kv_filter(data: Dict[str, Any], k: str, v: str) -> Dict[str, Any]:
    if v == "":
        data = {exp: args[k]["loc"] for exp, args in data.items() if k in args}
        return pd.DataFrame.from_dict(data, orient="index", columns=[k])

    to_del = []
    for exp in list(data.keys()):
        t = None
        val = data[exp].get(k)
        if isinstance(val, dict):
            t = val.get("loc")
            data[exp] = val.get("args", {})
        elif isinstance(val, (int, str)):
            t = str(val)
        if t != v:
            to_del.append(exp)
    for d in to_del:
        data.pop(d, None)
    return data


@overload
def filter_configs(
    query: str,
    ids: List[str],
    loader_func: Callable[[str], Dict[str, Any]],
    params: Literal[True],
) -> pd.DataFrame: ...


@overload
def filter_configs(
    query: str,
    ids: List[str],
    loader_func: Callable[[str], Dict[str, Any]],
    params: Literal[False] = False,
) -> List[str]: ...


def filter_configs(
    query: str,
    ids: List[str],
    loader_func: Callable[[str], Dict[str, Any]],
    params: bool = False,
) -> Union[List[str], pd.DataFrame]:
    """
    Filter and extract information from a collection of configurations.
    """
    qry = query.split(">")
    data = {i: deepcopy(loader_func(i)) for i in ids}

    for q in qry:
        if q == "":
            for _, exp in data.items():
                return list(exp.keys())
        elif "=" in q:
            k, v = q.split("=")
            result = _apply_kv_filter(data, k, v)
            if isinstance(result, pd.DataFrame):
                return result
            data = result
        else:
            data = _apply_key_filter(data, q)

    if params:
        data = _flatten_nested_locs(data)
        return pd.DataFrame.from_dict(data, orient="index")
    return list(data.keys())

def get_matching(
    base_id: str,
    get_ids_fn: Callable[[], List[str]],
    loader_fn: Callable[[str], Dict[str, Any]],
    query: str = None,
    include=False,
) -> Dict[str, List[str]]:
    """
    Get IDs of configurations that match the same flattened key-value pair(s) as a base config.

    Args:
        base_id (str): ID of the base configuration.
        get_ids_fn (Callable): Function to retrieve all configuration IDs.
        loader_fn (Callable): Function to load a configuration given its ID.
        query (str, optional): Specific query key or 'key=value' pair.

    Returns:
        Dict[str, List[str]]: Mapping of matched query to list of matching IDs.
    """

    def flatten(d, parent_key="", sep=">"):
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                if "loc" in v:
                    items[new_key] = v["loc"]
                elif "args" in v:
                    items.update(flatten(v["args"], new_key, sep=sep))
                else:
                    items.update(flatten(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items

    all_ids = [eid for eid in get_ids_fn() if eid != base_id]

    base_args = flatten(deepcopy(loader_fn(base_id)))

    # Load and flatten others
    flat_data = {}
    for eid in all_ids:
        obj = loader_fn(eid)
        flat_data[eid] = flatten(deepcopy(obj))

    result = {}

    if query:
        if "=" in query:
            key, val = query.split("=")
        else:
            key, val = query, base_args.get(query)

        if key not in base_args:
            return {}

        base_val = base_args.get(key)
        if val is not None and str(val) != str(base_val):
            return {}

        matched = [eid for eid, args in flat_data.items() if args.get(key) == base_val]
        if matched:
            result[f"{key}={base_val}"] = matched
    else:
        for key, base_val in base_args.items():
            matched = [
                eid for eid, args in flat_data.items() if args.get(key) == base_val
            ]
            if matched:
                result[f"{key}={base_val}"] = matched
    if include:
        for i in result:
            result[i] += [base_id]
    return result

