import sklearn, numpy as np, math, pandas, torch
import sys, os
from interfaces.IModel import IModel

class GamePredictionModel(IModel):
    def __init__(self):
        self.model = torch.jit.load('GamePredictions-53Epochs-Sampled.pt')

    def Predict(self, params:tuple) -> float:
        with torch.no_grad():
            homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homefenDiffToDate,awayFenDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate = params
            mean = np.load('mean.npy')
            stdev = np.load('stddev.npy')
            x = (np.array([homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homefenDiffToDate,awayFenDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate]) - mean) / stdev
            x = torch.from_numpy(x).float().to('cpu')
            pred = self.model(x)
            print(pred)
            return float(round(pred.item(), 2))