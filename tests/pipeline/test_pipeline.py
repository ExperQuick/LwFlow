# tests/integration/test_pipeline_run.py
import pytest
from lwflow.experiment import PipeLine
import os

def test_pipeline_integration_run(setup_lab_env):
    from lwflow.context import register_libs_path

    register_libs_path('./tests/comp_dir')

    # Initialize new pipeline
    args = {
        'workflow':{
            'loc':"ygjyg.jhhgu",
            'args':{}
        },
        'args': {"model": {"loc": "Comps.Dummy_models.DummyModel"},
        "dataset": {"loc": "Comps.Dummy_datasets.DummyDataSet", "args": {}},
        "optimizer": {"loc": "Comps.Dummy_optimizers.DummyOptimizer", "args": {}},
        "loss": {"loc": "Comps.Dummy_losses.DummyLoss"},
        "metrics": {"accuracy": {"loc": "Comps.Dummy_metrics.DummyMetric"}},
        "train_batch_size": 2,
        "val_batch_size": 2,
        "train_data_src": "data/train",
        "val_data_src": "data/val"}
    }

    pipe = PipeLine()
    pplid = "integration_test"
    pipe.new(pplid=pplid, args=args)
    pipe.prepare()
    
    assert os.path.exists(pipe.get_path("config"))
    assert os.path.exists(pipe.get_path("quick"))

    # Simulate training
    pipe.update({
        "epoch": 1, "train_loss": 0.8, "val_loss": 0.5,
        "train_accuracy": 0.9, "val_accuracy": 0.85,
        "train_duration": 5.0, "val_duration": 1.0
    })

    quick_data = open(pipe.get_path("quick"))
    assert '"val_loss": 0.5' in quick_data.read()
