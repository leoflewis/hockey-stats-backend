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

sql = MYSQLConnection()
processor = GamePredictionEngine()
games = sql.GetUnPredictedGames20232024()

for game in games:
    print(game)
    processor.PredictGame(game)