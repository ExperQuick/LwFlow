Templates
=========


models
------

.. code-block:: python

    # Model
    import torch
    import torch.nn as nn
    from torch.nn import functional as F
    from PyTorchLabFlow.utils import Model

    class SimpleNN(Model):
        def __init__(self):
            super().__init__()
            self.args = {"h1_dim":None, "h2_dim":None,'drop':None}

        def _setup(self, args):
            h1_dim, h2_dim, drop = args['h1_dim'], args['h2_dim'], args['drop']
            self.seq = nn.Sequential(
                nn.Linear(14, h1_dim),
                nn.ReLU(),
                nn.Linear(h1_dim, h2_dim),
                nn.ReLU(),
                nn.Linear(h2_dim, h2_dim*2),
                nn.ReLU(),
                nn.Linear(h2_dim*2, h2_dim*2),
                nn.ReLU()
            )

            self.dropout = nn.Dropout(p=drop)
            self.final = nn.Linear(h2_dim*2, 1)

        def forward(self, x):
            x = self.seq(x)
            x = self.dropout(x)
            x = self.final(x)
            return x

    class SimpleNNe(Model):
        def __init__(self):
            super().__init__()
            self.args = {"embedding_info":None, "continuous_dim":None,'hidden_dim':None, 'drop':None}

        def _setup(self, args):
            embedding_info, continuous_dim, hidden_dim,drop = args['embedding_info'], args['continuous_dim'], args['hidden_dim'], args['drop']
            self.embeddings = nn.ModuleList([
                nn.Embedding(num_categories, emb_dim)
                for num_categories, emb_dim in embedding_info
            ])

            self.continuous_dim = continuous_dim
            total_emb_dim = sum(emb_dim for _, emb_dim in embedding_info)

            self.fc = nn.Sequential(
                nn.Linear(total_emb_dim + continuous_dim, hidden_dim),
                nn.ReLU(),
                nn.Dropout(drop),
                nn.Linear(hidden_dim, 1)
            )

        def forward(self, x_cat, x_cont):
            x = [emb(x_cat[:, i]) for i, emb in enumerate(self.embeddings)]
            x = torch.cat(x, dim=1)
            x = torch.cat([x, x_cont], dim=1)
            return self.fc(x)


datasets
--------

.. code-block:: python

    from PyTorchLabFlow.utils import DataSet

    import torch

    import pandas as pd
    class DS01(DataSet):
        def __init__(self):
            self.args = {"data_src":None}

        def _setup(self, args):
            self.df = pd.read_csv(args['data_src'])

        def __len__(self):
            return len(self.df)

        def __getitem__(self, idx):
            row = self.df.iloc[idx, :].values
            row = torch.tensor(row, dtype=torch.float32)  # Convert entire row to float32 tensor
            label = row[-1]
            data = row[:-1]
            return [data], [label]


    import pandas as pd
    import torch
    class DS02(DataSet):
        def __init__(self):
            self.args = {"data_src":None}

        def _setup(self, args):

            self.df = pd.read_csv(args['data_src'])
            self.df.replace('?', pd.NA, inplace=True)
            self.df = self.df.dropna()
            # Define categorical and continuous columns
            self.cat_cols = [
                'workclass', 'education', 'marital_status', 'relationship', 'race',
                'occupation', 'native_country'
            ]
            self.cont_cols = [
                'age', 'fnlwgt', 'education_num', 'capital_gain', 'capital_loss', 'hours_per_week'
            ]
            self.label_col = 'income'

            # Define mappings for categorical columns (ensure this matches your earlier mappings)
            self.label_encoders = {
                'workclass': {
                    'Private': 0, 'Local-gov': 1, 'Self-emp-not-inc': 2, 'Federal-gov': 3,
                    'State-gov': 4, 'Self-emp-inc': 5, 'Without-pay': 6, 'Never-worked': 7
                },
                'education': {
                    '11th': 0, 'HS-grad': 1, 'Assoc-acdm': 2, 'Some-college': 3, '10th': 4,
                    'Prof-school': 5, '7th-8th': 6, 'Bachelors': 7, 'Masters': 8, '5th-6th': 9,
                    'Assoc-voc': 10, '9th': 11, 'Doctorate': 12, '12th': 13, '1st-4th': 14, 'Preschool': 15
                },
                'marital_status': {
                    'Never-married': 0, 'Married-civ-spouse': 1, 'Widowed': 2,
                    'Divorced': 3, 'Separated': 4, 'Married-spouse-absent': 5, 'Married-AF-spouse': 6
                },
                'relationship': {
                    'Own-child': 0, 'Husband': 1, 'Not-in-family': 2,
                    'Unmarried': 3, 'Wife': 4, 'Other-relative': 5
                },
                'race': {
                    'Black': 0, 'White': 1, 'Other': 2, 'Amer-Indian-Eskimo': 3, 'Asian-Pac-Islander': 4
                },
                'occupation': {
                    'Machine-op-inspct': 0, 'Farming-fishing': 1, 'Protective-serv': 2,
                    'Other-service': 3, 'Prof-specialty': 4, 'Craft-repair': 5,
                    'Adm-clerical': 6, 'Exec-managerial': 7, 'Tech-support': 8,
                    'Sales': 9, 'Priv-house-serv': 10, 'Transport-moving': 11,
                    'Handlers-cleaners': 12, 'Armed-Forces': 13
                },
                'native_country': {
                    'United-States': 0, 'Peru': 1, 'Guatemala': 2, 'Mexico': 3, 'Dominican-Republic': 4,
                    'Ireland': 5, 'Germany': 6, 'Philippines': 7, 'Thailand': 8, 'Haiti': 9, 'El-Salvador': 10,
                    'Puerto-Rico': 11, 'Vietnam': 12, 'South': 13, 'Columbia': 14, 'Japan': 15, 'India': 16,
                    'Cambodia': 17, 'Poland': 18, 'Laos': 19, 'England': 20, 'Cuba': 21, 'Taiwan': 22,
                    'Italy': 23, 'Canada': 24, 'Portugal': 25, 'China': 26, 'Nicaragua': 27, 'Honduras': 28,
                    'Iran': 29, 'Scotland': 30, 'Jamaica': 31, 'Ecuador': 32, 'Yugoslavia': 33, 'Hungary': 34,
                    'Hong': 35, 'Greece': 36, 'Trinadad&Tobago': 37, 'Outlying-US(Guam-USVI-etc)': 38,
                    'France': 39, 'Holand-Netherlands': 40
                }
            }

            # Encode categorical variables
            for col, mapping in self.label_encoders.items():
                self.df[col] = self.df[col].replace(mapping)

            # Encode label column
            self.df[self.label_col] = self.df[self.label_col].replace({'<=50K': 0, '>50K': 1})

            # Convert everything to torch tensors
            self.cat_data = torch.tensor(self.df[self.cat_cols].values, dtype=torch.long)
            self.cont_data = torch.tensor(self.df[self.cont_cols].values, dtype=torch.float32)
            self.labels = torch.tensor(self.df[self.label_col].values, dtype=torch.float32).unsqueeze(1)
        def __len__(self):
            return len(self.df)

        def __getitem__(self, idx):
            label = self.labels[idx]
            return [self.cat_data[idx], self.cont_data[idx]], [label]

losses
------

.. code-block:: python
    #Loss
    from PyTorchLabFlow.utils import Loss
    from torch import nn

    class BCElogit(Loss):
        def __init__(self):
            super().__init__()
            self.args ={}
        def _setup(self,args):
            self.criterion = nn.BCEWithLogitsLoss()

        def forward(self, logits, y_true):
            # y_true = y_true[0]
            # print(16, logits.shape,y_true.shape)
            logits = logits.view_as(y_true)
            loss = self.criterion(logits, y_true.float())
            # print(16, logits.shape,y_true.shape, loss)
            return loss


optimizers
----------

.. code-block:: python

    from PyTorchLabFlow.utils import Optimizer
    import torch.optim as optim

    class OptAdam(Optimizer):
        def __init__(self):
            super().__init__()
            self.optimizer = None

        def _setup(self,args):
            learning_rate = args.get('learning_rate', 0.001)
            self.optimizer = optim.Adam(args['model_parameters'], lr=learning_rate)
        def step(self, **kwargs):
            # Step function to apply the gradients and update model parameters
            self.optimizer.step()

        def zero_grad(self):
            # Zero the gradients before the backward pass
            self.optimizer.zero_grad()


metrics
--------

.. code-block:: python

    # metrics

    import torch
    from PyTorchLabFlow.utils import Metric
    from torchmetrics.classification import BinaryAccuracy

    class BinAcc(Metric):
        def __init__(self):
            super().__init__()
            # self.args = {'threshold':None}

        def _setup(self, args):
            thres = args.get('threshold',0.5)
            self.accuracy = BinaryAccuracy(threshold=thres)

        def forward(self,y_pred, y_true):
            y_pred = y_pred.view_as(y_true)
            accuracy = self.accuracy(y_pred, y_true)
            return accuracy.item()


    import torch.nn as nn
    from sklearn.metrics import roc_auc_score
    class AUROC(Metric):
        def __init__(self):
            super().__init__()
        def _setup(self, args):
            pass
        def forward(self, outputs, targets):
            if outputs.size(1) == 1:
                probabilities = torch.sigmoid(outputs).detach().cpu().numpy()
                targets = targets.detach().cpu().numpy()
                # print(targets.shape, probabilities.shape)
                auroc = roc_auc_score(targets, probabilities)
            # For multi-class classification (softmax)
            else:
                probabilities = torch.softmax(outputs, dim=1).detach().cpu().numpy()
                targets = targets.detach().cpu().numpy()
                # One-hot encode targets for multi-class
                auroc = roc_auc_score(targets, probabilities, average='macro', multi_class='ovr')

            return auroc

    from sklearn.metrics import average_precision_score

    class AUPRC(Metric):
        def __init__(self):
            super().__init__()
        def _setup(self, args):
            pass
        def forward(self, outputs, targets):
            if outputs.size(1) == 1:
                probabilities = torch.sigmoid(outputs).detach().cpu().numpy()
                targets = targets.detach().cpu().numpy()
                auprc = average_precision_score(targets, probabilities)
            # For multi-class classification (softmax)
            else:
                probabilities = torch.softmax(outputs, dim=1).detach().cpu().numpy()
                targets = targets.detach().cpu().numpy()
                # For multi-class, use average_precision_score for each class separately and average
                auprc = average_precision_score(targets, probabilities, average='macro', multi_class='ovr')

            return auprc

    from sklearn.metrics import f1_score
    class F1Score(Metric):
        def __init__(self):
            super().__init__()
        def _setup(self, args):
                pass
        def forward(self, outputs, targets):
            if outputs.size(1) == 1:
                probabilities = torch.sigmoid(outputs).detach().cpu().numpy()
                predictions = (probabilities > 0.5).astype(int)  # Convert to 0 or 1 (binary classification)
                targets = targets.detach().cpu().numpy()
                f1 = f1_score(targets, predictions)
            # For multi-class classification (softmax)
            else:
                probabilities = torch.softmax(outputs, dim=1).detach().cpu().numpy()
                predictions = probabilities.argmax(axis=1)  # Choose the class with the highest probability
                targets = targets.detach().cpu().numpy()
                f1 = f1_score(targets, predictions, average='macro')  # Macro-average for multi-class
            return f1


