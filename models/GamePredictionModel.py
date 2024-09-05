import sklearn, numpy, math, pandas
from joblib import load
from interfaces.IModel import IModel

class GamePredictionModel(IModel):
    def __init__(self):
        self.model = load('gamePrediction.joblib')
        self.predictors = ["homexGDiff", "awayxGdiff",  "homeShotDiff",  "awayShotDiff",  "homeFenDiff",  "awayFenDiff",  "homeGoalDiff",  "awayGoalDiff"]
    
    def Predict(self, params:tuple):
        homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homefenDiffToDate,awayFenDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate = params
        stats = [[homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homefenDiffToDate, awayFenDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate]]
        new_df = pandas.DataFrame(stats, columns=self.predictors)
        pred = self.model.predict_proba(new_df)
        return round(pred, 2)