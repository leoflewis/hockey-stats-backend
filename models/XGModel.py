import sklearn, numpy, math, pandas, os
from joblib import load

class XGModel():
    def __init__(self):
        self.model = load('xG.joblib') 
        self.predictors = ['xC', 'yC', 'Rebound', 'Power Play', 'Type_', 'Type_BACKHAND', 'Type_DEFLECTED', 'Type_SLAP SHOT', 'Type_SNAP SHOT', 'Type_TIP-IN', 'Type_WRAP-AROUND', 'Type_WRIST SHOT', 'Angle_Radians', 'Angle_Degrees', 'Distance']

    def Predict(self, params: tuple):
        x, y, shotType, rebound = params
        new_angles = self.GetAngles(x, y)
        new_distance = self.GetDistance(x, y)
        if shotType == 'wrist':
            new_shot = [[x, y, 0, 0, 0, 0, 0, 0, 0, 1, new_angles[0], new_angles[1], new_distance]]
        elif shotType == 'backhand':
            new_shot = [[x, y, 0, 1, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
        elif shotType == 'deflected':
            new_shot = [[x, y, 0, 0, 1, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
        elif shotType == 'slap':
            new_shot = [[x, y, 0, 0, 0, 1, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
        elif shotType == 'snap':
            new_shot = [[x, y, 0, 0, 0, 0, 1, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
        elif shotType == 'tip-in':
            new_shot = [[x, y, 0, 1, 0, 0, 0, 1, 0, 0, new_angles[0], new_angles[1], new_distance]]
        elif shotType == 'wrap-around':
            new_shot = [[x, y, 0, 0, 0, 0, 0, 0, 1, 0, new_angles[0], new_angles[1], new_distance]]
        else:
            new_shot = [[x, y, 1, 0, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
        if rebound:
            new_shot[0].insert(2, 1)
        else:
            new_shot[0].insert(2, 0)
        new_shot[0].insert(3, 0)
        new_df = pandas.DataFrame(new_shot, columns=self.predictors)
        pred = self.model.predict_proba(new_df)
        pred = round(pred[0][1], 4)
        return pred


    def GetAngles(self, x, y):
        if x == 89 and y == 0: return [0,0]
        num = math.sqrt(((89.0 - x) * (89.0 - x)) + ((y) * (y))) 
        radians = numpy.arcsin(y/num)
        degrees = (radians * 180.0) / 3.14
        arr = [radians, degrees]
        return arr
    
    def GetDistance(self, x, y):
        return numpy.sqrt((y - 0)**2 + (x - 89.0)**2)