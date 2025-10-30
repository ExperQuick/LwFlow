Overview
========

PyTorchLabFlow is a modular deep learning framework designed for building flexible, reusable, and composable deep learning pipelines.

In PyTorchLabFlow, **everything is a Component**—from models and datasets to losses, optimizers, and custom blocks. This modular design enables users to configure, nest, and reuse components dynamically, simplifying the construction and maintenance of complex deep learning workflows.

Component
---------

- A **Component** is a reusable, self-contained code block that can represent a model, dataset, optimizer, loss function, or any processing step.
- Each Component is identified by:
  - ``loc``: a string specifying the Python import path of the component class.
  - ``args``: a dictionary containing parameters to configure or initialize the component.
- Components support **infinite nesting**, allowing other components to be passed as parameters inside ``args``.

Key Features
~~~~~~~~~~~~

- **Dynamic Loading**: Components are loaded at runtime using the ``loc`` string and initialized with ``args``.
- **Argument Validation**: Each component declares required arguments which are validated during setup.
- **Deep Nesting**: Components can include other components inside their ``args`` dictionary for flexible pipeline composition.

Core Base Class: ``Component``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The abstract ``Component`` class provides core setup logic:

- ``check_args(args: dict)``: Checks if all required arguments are provided.
- ``setup(args: dict)``: Validates arguments and calls the subclass's ``_setup()`` method to build the component.

Specialized Component Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: Component Overview
   :widths: 20 20 60
   :header-rows: 1

   * - Type
     - Class
     - Inherits From
   * - Model
     - ``Model``
     - ``Component``, ``torch.nn.Module``
   * - Dataset
     - ``DataSet``
     - ``Component``, ``torch.utils.data.Dataset``
   * - Loss
     - ``Loss``
     - ``Component``, ``torch.nn.Module``
   * - Optimizer
     - ``Optimizer``
     - ``Component``, ``torch.nn.Module``
   * - Metric
     - ``Metric``
     - ``Component``, ``torch.nn.Module``


- **DataSet**: feeds  data to model from files, you can fuse processing steps inside it.
- **Model**: conventional `torch.nn.Module`  but  `PyTorchLabFlow`'s  wrapper,
- **Metric**: your megtric functions otherthan loss
- **Loss**: the loss function, grad calculation
- **Optimizer**: your optimizer



Example: Defining a Custom Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    class MyModel(Model):
        def __init__(self):
            super().__init__()
            self.args = {"input_dim", "output_dim"}

        def _setup(self, args):
            self.linear = nn.Linear(args["input_dim"], args["output_dim"])
            return self

Nesting Components Example
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    {
        "loc": "my_project.models.TransformerClassifier",
        "args": {
            "backbone": {
                "loc": "my_project.models.TransformerEncoder",
                "args": {
                    "dim": 256,
                    "depth": 6
                }
            },
            "num_classes": 10
        }
    }


Pipeline
---------

A Pipeline orchestrates and manages all components of a machine learning experiment. It serializes the experiment's configuration, including the model, dataset, and hyperparameters, into a unique, identifiable setup. Each pipeline instance, identified by a unique expid, ensures that experiments are reproducible and their configurations are unique.

To initialize a new experiment from scratch, you can use the new() method.

.. code-block:: python
  # Create a new pipeline from scratch
  P = PipeLine()
  P.new(pplid='my_first_experiment', args=experiment_args)


Alternatively, if you wish to create a new experiment based on a previous one, perhaps to fine-tune a model or slightly alter hyperparameters, you can use the use() method. This inherits the configuration from an existing experiment, allowing you to apply specific changes.

.. code-block:: python

  # Create a new pipeline based on an existing one
  P = PipeLine()
  P.use(use_exp='my_first_experiment', expid='fine_tuned_experiment', args=new_args)


