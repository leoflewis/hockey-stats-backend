import sys, os
os.chdir("..")
sys.path.append(os.getcwd())

from hockeylogic.ProcessGameEvents import ProcessGameEvents
from interfaces.IMYSQLService import IMYSQLService
from services.MYSQLService import MYSQLConnection
from services.NHAPIService import NHLApi
from datetime import datetime 
from hockeylogic.PredictGames import GamePredictionEngine
import pandas 

predictors = ["homexGDiff", "awayxGdiff",  "homeShotDiff",  "awayShotDiff",  "homeFenDiff",  "awayFenDiff",  "homeGoalDiff",  "awayGoalDiff", "result"]
processor = GamePredictionEngine()
nhl = NHLApi()
games = [
    {
        "awayTeam":{
            "abbrev": "CAR",
            "id": 12
        },
        "homeTeam":{
            "abbrev": "CHI",
            "id": 16
        }
    },
    {
        "awayTeam":{
            "abbrev": "CHI",
            "id": 16
        },
        "homeTeam":{
            "abbrev": "CAR",
            "id": 12
        }
    },
    ]


for game in games:
    print(game['awayTeam']['abbrev'], "at", game['homeTeam']['abbrev'])
    processor.today = datetime.now().strftime('%Y-%m-%d')
    processor.season = 20232024
    processor.homeTeamId = game['homeTeam']['id']
    processor.awayTeamId = game['awayTeam']['id']

    homexGDiffToDate, awayxGDiffToDate = processor.GetXG()
    
    homeShotDiffToDate, awayShotDiffToDate = processor.GetShots()
    
    homeGoalDiffToDate, awayGoalDiffToDate = processor.GetGoals()
    
    homefenDiffToDate, awayFenDiffToDate = processor.GetFenwick()
        
    result = processor.model.Predict((homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate, homefenDiffToDate, awayFenDiffToDate))
    
    print(round(result * 100, 2), "%")
    
    #processor.sql.UpdateGameWithPrediction(gameId, result)