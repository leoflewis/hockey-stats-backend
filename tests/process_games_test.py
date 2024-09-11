import sys, os
os.chdir("..")
sys.path.append(os.getcwd())

from hockeylogic.ProcessGameEvents import ProcessGameEvents
from interfaces.IMYSQLService import IMYSQLService
from services.MYSQLService import MYSQLConnection
from datetime import datetime 
from hockeylogic.PredictGames import GamePredictionEngine
import pandas 

predictors = ["homexGDiff", "awayxGdiff",  "homeShotDiff",  "awayShotDiff",  "homeFenDiff",  "awayFenDiff",  "homeGoalDiff",  "awayGoalDiff", "result"]
processor = GamePredictionEngine()

allrows = []
conn = MYSQLConnection()
for game in conn.GetAllGamesInBatches():
    print(game[1])
    homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate, homefenDiffToDate, awayFenDiffToDate = processor.ProduceParameters(game)
    stats = [homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homefenDiffToDate, awayFenDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate, game[5]]
    allrows.append(stats)
print(allrows)
df = pandas.DataFrame(allrows, columns=predictors)

df.to_csv("22-23-24-data.csv")

conn.Close()