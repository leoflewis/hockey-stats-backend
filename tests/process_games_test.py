import sys, os
os.chdir("..")
sys.path.append(os.getcwd())

from hockeylogic.ProcessGameEvents import ProcessGameEvents
from interfaces.IMYSQLService import IMYSQLService
from services.MYSQLService import MYSQLConnection
from datetime import datetime 
from services.NHAPIService import NHLApi

conn = MYSQLConnection()

nhlAPI = NHLApi()
processor = ProcessGameEvents(sql=conn) 

for game in nhlAPI.GetRegularSeasonByGame():
    if game["gameType"] != 2: continue
    print(game["id"], game["startTimeUTC"])
    processor.ProcessGame(gameId=game["id"])

conn.Close()