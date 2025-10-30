Workflows
=========

Setup your Project
------------------

This tutorial explains how to initialize a machine learning project using the
``PyTorchLabFlow`` package by providing a configuration dictionary and calling a single function.

Prerequisites
~~~~~~~~~~~~~

Make sure the ``PyTorchLabFlow`` package is installed and accessible in your Python environment:

.. code-block:: bash

    pip install PyTorchLabFlow  # Or use your own installation method

Step 1: Define Your Project Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a dctionary with same given key and respective values.

.. code-block:: python

    settings = {
        "project_name": "AdultIncomePrediction",
        "project_dir": "path/to/your/data/folder",
        "component_dir": "path/to/your/component/folder",
        "setting_path": "path/to/the/project/config/file.json",
        "metrics": ["accuracy", "auroc", "f1score", "auprc"],
        "strategy": {"monitor": "val_loss", "mode": "min"},
    }

.. note::

keys  and  the  values  are  explained  below.

Explanation of Settings
~~~~~~~~~~~~~~~~~~~~~~~

Here’s a breakdown of the keys in the ``settings`` dictionary:

- ``project_name``: A name for your project; used for display/logging purposes.
- ``project_dir``: Directory where input data and processed outputs will be stored.
- ``component_dir``: Folder to store your model components like network, loss function, optimizer, etc.
- ``setting_path``: Full path where the settings configuration (as JSON) will be saved.
- ``metrics``: A list of metric names you plan to evaluate (e.g., ``accuracy``, ``f1score``, etc.). These should match implemented metric components or be defined later.
- ``strategy``: Defines your model selection strategy (e.g., monitor ``val_loss`` with ``min`` to minimize validation loss).
- ``defaults``: A dictionary of default values or placeholders that will be filled later:

  - ``metrics``: Initially set to ``None`` for each; these can later point to custom implementations.
  - ``loss``: The loss function component (e.g., ``CrossEntropyLoss``).
  - ``optimizer``: Your optimizer name or object (e.g., ``Adam``).
  - ``train_data_src``: Path to training data file.
  - ``valid_data_src``: Path to validation data file.
  - ``train_batch_size``, ``valid_batch_size``: Batch sizes for training and validation phases.

You can leave most values as ``None`` initially and fill them in after the project structure is generated.

Step 2: Run the Project Creation Function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``create_project`` function from ``PyTorchLabFlow.lab`` to initialize the project:

.. code-block:: python

    from PTLF.lab import create_project

    create_project(settings=settings)

What Happens Next?
~~~~~~~~~~~~~~~~~~

Running the script performs the following:

- Creates the specified ``data_path`` and ``component_dir`` folders if they don’t exist.
- Saves the ``settings`` dictionary as a JSON file at ``setting_path``.
- Initializes a basic folder structure for your ML project.

Resulting Directory Structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

    path/to/your/data/folder/
    │
    ├── Configs/               # Stores experiment configuration files
    ├── Weights/               # Stores trained model weights
    ├── Quicks/                # For quick experiments or debugging artifacts
    ├── Histories/             # Training history logs (loss/accuracy curves etc.)
    │
    ├── Archived/              # Stores older/archived experiment artifacts
    │   ├── Configs/
    │   ├── Weights/
    │   └── Histories/
    │
    ├── Transfer/              # For storing artifacts ready for deployment or sharing
    │   ├── Configs/
    │   ├── Weights/
    │   └── Histories/
    │
    ├── ppls.db                # SQLite DB for tracking experiment metadata
    └── settings.json          # Serialized configuration file used for setup


    path/to/your/component/folder/
    └── CompBase/                      # Base implementations of components
        ├── __init__.py
        ├── models.py                  # Model architectures (e.g., Meso4)
        ├── datasets.py               # Dataset handling and transformations
        ├── metrics.py                # Evaluation metrics (e.g., Accuracy, AUROC)
        ├── losses.py                 # Loss functions
        └── optimizers.py             # Optimizers (e.g., Adam)

Using the Project Later
~~~~~~~~~~~~~~~~~~~~~~~~

Once your project has been set up, you can load the full configuration and prepare the environment anytime using:

.. code-block:: python

    from PTLF.lab import lab_setup

    lab_setup(settings_path="path/to/the/project/config/file.json")

This sets up the internal context, links components, and restores all paths, making it easy to continue working in Jupyter notebooks, scripts, or any Python environment.

.. important::

   In any Jupyter notebook or Python script, simply call ``lab_setup`` at the top and you're ready to start working with the full project structure.

Summary
-------

+------------------+----------------------------------------+
| **Step**         | **Description**                        |
+==================+========================================+
| Install          | ``pip install PyTorchLabFlow``         |
+------------------+----------------------------------------+
| Create Config    | Define the ``settings`` dictionary     |
+------------------+----------------------------------------+
| Initialize       | Call ``create_project(settings)``      |
+------------------+----------------------------------------+
| Verify           | Ensure files and folders are created   |
+------------------+----------------------------------------+
| Load Later       | Use ``lab_setup(setting_path)``        |
+------------------+----------------------------------------+

You’re now ready to start building models, managing experiments, and scaling your ML workflow using the ``PyTorchLabFlow`` environment.


Building and Running Deep Learning Pipelines
---------------------------------------------
Step 1: Design your components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Design your `Dataset` class inheriting `PTLF.utils.DataSet`
2. Design your `model` inheriting `PTLF.utils.Model`
3. Make other components like `Loss`, `Optimizer`, and  one or more metrics what ever you decided while initiating a project


.. tip::
    See the full design notebook: `Design <Examples/design.html>`__





Step 2: Define Experiment Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a nested dictionary specifying all pipeline components such as model, dataset, optimizer, loss, metrics, and data sources.

.. code-block:: python

    expargs = {
        "dataset": {
            "loc": "CompBase.datasets.DS01",
            "args": {}
        },
        "model": {
            "loc": "CompBase.models.SimpleNN",
            "args": {
                "h1_dim": 120,
                "h2_dim": 1000,
                "drop": 0.3
            }
        },
        "loss": {
            "loc": "CompBase.losses.BCElogit",
            "args": {}
        },
        "optimizer": {
            "loc": "CompBase.optimizers.OptAdam",
            "args": {}
        },
        "metrics": {
            "accuracy": {
                "loc": "CompBase.metrics.BinAcc",
                "args": {}
            },
            "auroc": {
                "loc": "CompBase.metrics.AUROC",
                "args": {}
            }
        },
        "train_data_src": "path/to/train.csv",
        "val_data_src": "path/to/valid.csv",
        "train_batch_size": 36,
        "val_batch_size": 36
    }

Step 4: Initialize the Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    P = PipeLine()

Step 4: (Optional) Match Existing Experiments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check if this configuration has been used before:

.. code-block:: python

    P.match_args(expargs)
    # Returns existing experiment ID or False if new

Step 5: Create a New Experiment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    P.new(args=expargs.copy(), expid="exp2")

Step 6: Start Training
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    P.train(num_epochs=10)

Supports features like early stopping, verbose logging, and hooks.

Extra Utilities
~~~~~~~~~~~~~~~

.. list-table:: Pipeline Utilities
   :widths: 30 70
   :header-rows: 1

   * - Function
     - Description
   * - ``P.load(expid)``
     - Load existing experiment configuration.
   * - ``P.prepare()``
     - Prepare model, data, and metrics manually.
   * - ``P.load_model(epoch=5)``
     - Load model weights from a specific or best epoch.
   * - ``P.update(data)``
     - Log metrics after an epoch (usually called automatically).
   * - ``P.use(...)``
     - Create a new experiment based on an existing one.


Step 7: Plot Comparative Performances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from PTLF.experiment import plot_metrics

    Vs = plot_metrics(ppls=[...], metrics=['train_loss', "train_accuracy"])
    Vs["train_accuracy"]









