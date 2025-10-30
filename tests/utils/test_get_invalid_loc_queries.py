import pytest
from typing import List, Dict, Union
from lwflow.utils import get_invalid_loc_queries  # Update path as needed


def test_valid_locs_only():
    data = {
        "component1": {"loc": "module.Class1"},
        "component2": {
            "loc": "module.Class2",
            "args": {
                "child": {"loc": "module.SubClass"}
            }
        },
        "list_components": [
            {"loc": "module.ListClass1"},
            {"loc": "module.ListClass2"}
        ]
    }
    assert get_invalid_loc_queries(data) == []


def test_top_level_invalid_loc():
    data = {
        "component1": {"loc": "NoDotPath"},
        "component2": {"loc": "module.Class2"}
    }
    assert get_invalid_loc_queries(data) == ["component1"]


def test_nested_invalid_loc():
    data = {
        "component": {
            "loc": "module.Class",
            "args": {
                "child": {"loc": "InvalidLoc"}
            }
        }
    }
    assert get_invalid_loc_queries(data) == ["component>args>child"]


def test_list_nested_invalid():
    data = {
        "components": [
            {"loc": "Valid.Class"},
            {"loc": "InvalidLoc"},
            {"not_a_component": True},
        ]
    }
    assert get_invalid_loc_queries(data) == ["components[1]"]


def test_mixed_valid_invalid_deep():
    data = {
        "root": {
            "loc": "Main.Component",
            "args": {
                "children": [
                    {"loc": "Valid.Child"},
                    {"loc": "BadLoc"},
                    {
                        "loc": "Another.Component",
                        "args": {
                            "deep": {"loc": "NoDot"}
                        }
                    }
                ]
            }
        }
    }
    expected = ["root>args>children[1]", "root>args>children[2]>args>deep"]
    assert sorted(get_invalid_loc_queries(data)) == sorted(expected)


def test_empty_dict():
    assert get_invalid_loc_queries({}) == []


def test_empty_list():
    assert get_invalid_loc_queries([]) == []


def test_no_loc_present():
    data = {
        "something": {
            "args": {
                "other": {"value": 123}
            }
        }
    }
    assert get_invalid_loc_queries(data) == []


def test_loc_key_but_not_string():
    data = {
        "bad": {"loc": None},
        "bad2": {"loc": 123},
        "ok": {"loc": "mod.ok.Class"}
    }
    assert get_invalid_loc_queries(data) == ["bad", "bad2"]
