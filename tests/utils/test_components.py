import pytest
from typing import Dict, Any, Optional

from abc import ABC

from lwflow.utils import Component


class ComponentTestSubclass(Component):
    """Concrete subclass for testing Component base class."""

    def __init__(self):
        super().__init__()
        self.args = ["foo", "bar"]
        self.setup_called = False
        self.received_args = None

    def _setup(self, args: Dict[str, Any], P=None) -> Optional[Any]:
        self.setup_called = True
        self.received_args = args

def test_component_init_and_check_args():
    comp = ComponentTestSubclass()
    assert comp.loc == "ComponentTestSubclass"
    assert comp.args == ["foo", "bar"]

    # Missing args keys
    assert comp.check_args({"foo": 1}) is False
    assert comp.check_args({"foo": 1, "bar": 2}) is True


def test_component_setup_success():
    comp = ComponentTestSubclass()
    result = comp.setup({"foo": 1, "bar": 2})

    assert result == comp
    assert comp.setup_called is True
    assert comp.received_args == {"foo": 1, "bar": 2}


def test_component_setup_fails_bad_args():
    comp = ComponentTestSubclass()
    with pytest.raises(ValueError):
        comp.setup({"foo": 1})  # Missing 'bar'


def test_component_setup_missing_setup_method():
    class NoSetupComponent(Component):
        pass

    comp = NoSetupComponent()
    comp.args = []  # no required args
    with pytest.raises(NotImplementedError):
        comp.setup({})



def test_component_private_setup_not_implemented():
    class DummyComponent(Component):
        def __init__(self):
            super().__init__()
            self.args = []

    comp = DummyComponent()
    with pytest.raises(NotImplementedError):
        comp._setup({})

