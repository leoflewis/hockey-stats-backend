from services.NHAPIService import NHLApi
from hockeylogic.ProcessGameEvents import ProcessGameEvents
from services.MYSQLService import MYSQLConnection

nhl = NHLApi()
mysql = MYSQLConnection()
processor = ProcessGameEvents(mysql)

for game in nhl.GetRegularSeasonByGame(startDate="2025-10-07", endDate="2025-12-24"):
    if game['gameType'] == 2:
        print(game)
        processor.ProcessGame(game['id'])
        break