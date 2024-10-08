import torch, math
import pandas as pd
import torch.utils.data as data_utils
from torch import nn
from torch import optim
import numpy as np
import matplotlib.pyplot as plt

torch.manual_seed(97)

class NeuralNetwork(nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim):
        super(NeuralNetwork, self).__init__()
        self.layer_1 = nn.Linear(input_dim, hidden_dim)
        self.layer_2 = nn.Linear(hidden_dim, hidden_dim)
        self.layer_3 = nn.Linear(hidden_dim, hidden_dim)
        self.layer_4 = nn.Linear(hidden_dim, output_dim)
       
    def forward(self, x):
        x = torch.nn.functional.relu(self.layer_1(x))
        x = torch.nn.functional.relu(self.layer_2(x))
        x = torch.nn.functional.relu(self.layer_3(x))
        x = torch.nn.functional.sigmoid(self.layer_4(x))
        return x

class GameData:
    def __init__(self, batch_size):
        data = pd.read_csv("..\\22-23-24-data.csv", header=0, index_col=0)
        self.x = data[["homexGDiff","awayxGdiff","homeShotDiff","awayShotDiff","homeFenDiff","awayFenDiff","homeGoalDiff","awayGoalDiff"]]
        self.y = data["result"]
        self.standardize_x()
        self.batch_size = batch_size
        self.x_train = torch.from_numpy(self.x[:2100].values).float().to('cpu')
        self.x_test = torch.from_numpy(self.x[2100:].values).float().to('cpu')
        self.y_train = torch.from_numpy(self.y[:2100].values).float().to('cpu')
        self.y_test = torch.from_numpy(self.y[2100:].values).float().to('cpu')

    def standardize_x(self):
        self.x = (self.x - self.x.mean()) / self.x.std()

    def train_loader(self) -> data_utils.DataLoader:
        train = data_utils.TensorDataset(self.x_train, self.y_train)
        return data_utils.DataLoader(train, batch_size=self.batch_size, shuffle=True)
    
    def test_loader(self) -> data_utils.DataLoader:
        test = data_utils.TensorDataset(self.x_test, self.y_test)
        return data_utils.DataLoader(test, batch_size=self.batch_size, shuffle=True)

class Orchestrator():
    def __init__(self, epochs, lr, neurons, batch_size):
        self.model = NeuralNetwork(8, neurons, 1)
        print(self.model)
        self.loss_fn = nn.BCELoss()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)
        self.loss_values = []
        self.val_accuracy = []
        self.train_accuracy = []
        self.data = GameData(batch_size)
        self.epochs = epochs
        self.batch_size = batch_size
    
    def train(self): 
        train = self.data.train_loader()
        test = self.data.test_loader()
        for epoch in range(self.epochs):
            train_samples, train_correct = 0, 0
            for X, y in train:
                train_samples += len(X)
                self.optimizer.zero_grad()
                pred = self.model(X)
                train_correct += (np.where(pred.reshape(y.shape) > 0.5, 1, 0) == y).sum().item()
                loss = self.loss_fn(pred, y.unsqueeze(-1))
                self.loss_values.append(loss.item())
                loss.backward()
                self.optimizer.step()
            self.train_accuracy.append(train_correct / train_samples)
            
            with torch.no_grad():
                val_samples, val_correct = 0, 0
                for x, y in test:
                    val_samples += len(x) 
                    raw_predictions = self.model(x)
                    predictions = np.where(raw_predictions.reshape(y.shape) > 0.5, 1, 0)
                    loss = self.loss_fn(raw_predictions, y.unsqueeze(-1))
                    val_correct += (predictions == y).sum().item()
                self.val_accuracy.append(val_correct / val_samples)
                print(f"Epoch {epoch}, {round(val_correct / val_samples, 2)}% validation accuracy, validation loss: {loss.item()}")
        
    def plot_scores(self):
        step = np.linspace(0, self.epochs, self.epochs * math.ceil(2100/self.batch_size)) # batches per epoch
        plt.figure()
        plt.title("Training step wise loss")
        plt.xlabel("Epochs")
        plt.ylabel("Loss")
        plt.plot(step, np.array(self.loss_values))
        plt.figure()
        plt.plot(range(0,self.epochs), np.array(self.val_accuracy), label='validation')
        plt.plot(range(0,self.epochs), np.array(self.train_accuracy), label='train')
        plt.title("Accuracy")
        plt.xlabel("Epochs")
        plt.ylabel("Accuracy")
        plt.legend()
        plt.show()

test = Orchestrator(150, .01, 248, 700) 
test.train()
test.plot_scores()