
from lwflow.utils import Component, WorkFlow
import time
from pathlib import Path

class DummyComp(Component):
    def __init__(self):
        Component.__init__()
    
    def _setup(self, args):
        self.linear = lambda x: x**2

    def forward(self, x):
        return self.linear(x)
    

class Wf1(WorkFlow):
    def __init__(self, loc = None):
        super().__init__(loc)

    def run(self):
        time.sleep(100)


    def prepare(self):
        pass

    def new(self):
        return True
    
    def get_path(self,  of: str,
        pplid = None,
        args = None):
        return of / f"{pplid}.json"

