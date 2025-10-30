import sys
import types
import warnings
import pytest

from lwflow.utils import load_component, ComponentLoadError, Component


class DummyComp(Component):
    def __init__(self):
        super().__init__()
        self.setup_called = False

    def _setup(self, args):
        self.setup_called = True
        return self


class DummyNoSetup:
    pass


def test_load_component_existing_module(monkeypatch):
    # Create dummy module with DummyComp
    dummy_module = types.ModuleType("some.module")
    dummy_module.DummyComp = DummyComp

    # Patch importlib.import_module to return dummy module
    monkeypatch.setattr("importlib.import_module", lambda name: dummy_module)

    comp = load_component("some.module.DummyComp")
    assert isinstance(comp, DummyComp)
    assert comp.setup_called


def test_load_component_setup_false(monkeypatch):
    dummy_module = types.ModuleType("some.module")
    dummy_module.DummyComp = DummyComp
    monkeypatch.setattr("importlib.import_module", lambda name: dummy_module)

    comp = load_component("some.module.DummyComp", setup=False)
    assert isinstance(comp, DummyComp)
    assert comp.setup_called is False  # setup should NOT be called


def test_load_component_no_setup_method(monkeypatch):
    dummy_module = types.ModuleType("some.module")
    dummy_module.DummyNoSetup = DummyNoSetup
    monkeypatch.setattr("importlib.import_module", lambda name: dummy_module)

    comp = load_component("some.module.DummyNoSetup")
    assert isinstance(comp, DummyNoSetup)


def test_load_component_no_dot_warn(monkeypatch):
    # Place DummyComp in __main__ for this test
    sys.modules["__main__"].DummyComp = DummyComp

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        comp = load_component("DummyComp")
        assert isinstance(comp, DummyComp)
        assert any("not saved" in str(warn.message) for warn in w)
    
    # Cleanup
    del sys.modules["__main__"].DummyComp


def test_load_component_invalid_class(monkeypatch):
    dummy_module = types.ModuleType("some.module")
    monkeypatch.setattr("importlib.import_module", lambda name: dummy_module)

    with pytest.raises(ComponentLoadError):
        load_component("some.module.MissingClass")


def test_load_component_import_error(monkeypatch):
    def raise_import(name):
        raise ImportError("No module")

    monkeypatch.setattr("importlib.import_module", raise_import)

    with pytest.raises(ImportError):
        load_component("non.existent.ModuleClass")
