import pytest
from lwflow.utils import extract_all_locs  # Adjust import path as needed

def test_extract_all_locs_simple():
    data = {"comp": {"loc": "module.Simple"}}
    expected = ["module.Simple"]
    assert extract_all_locs(data) == expected

def test_extract_all_locs_nested():
    data = {
        "model": {
            "loc": "module.Model",
            "args": {
                "encoder": {
                    "loc": "module.Encoder",
                    "args": {
                        "embedder": {
                            "loc": "module.Embedder"
                        }
                    }
                }
            }
        }
    }
    expected = ["module.Model", "module.Encoder", "module.Embedder"]
    assert sorted(extract_all_locs(data)) == sorted(expected)

def test_extract_all_locs_list_of_dicts():
    data = [
        {"loc": "module.Class1"},
        {"loc": "module.Class2", "args": {"sub": {"loc": "module.Sub"}}},
        {"not_loc": "ignore"},
    ]
    expected = ["module.Class1", "module.Class2", "module.Sub"]
    assert sorted(extract_all_locs(data)) == sorted(expected)

def test_extract_all_locs_mixed_nested():
    data = {
        "main": {
            "loc": "module.Main",
            "args": {
                "children": [
                    {"loc": "module.Child1"},
                    {"loc": "module.Child2", "args": {
                        "subchild": {"loc": "module.SubChild"}
                    }},
                ]
            }
        },
        "extra": [
            {"loc": "module.Extra"},
            [{"loc": "module.DeepExtra"}]
        ]
    }
    expected = [
        "module.Main",
        "module.Child1",
        "module.Child2",
        "module.SubChild",
        "module.Extra",
        "module.DeepExtra"
    ]
    assert sorted(extract_all_locs(data)) == sorted(expected)

def test_extract_all_locs_empty():
    assert extract_all_locs({}) == []
    assert extract_all_locs([]) == []

def test_extract_all_locs_no_loc_keys():
    data = {
        "thing": {
            "args": {
                "another": {
                    "stuff": "nope"
                }
            }
        },
        "list": [
            {"arg": 123},
            [{"foo": "bar"}]
        ]
    }
    assert extract_all_locs(data) == []
