from hockeylogic.ProcessPlayerStats import ProcessPlayerStats
from datetime import date
from services.MYSQLService import MYSQLConnection

mysql = MYSQLConnection()
processor = ProcessPlayerStats(mysql)

d = date(2024, 11, 25)
season = '20222023'
playerIds = mysql.GetPlayersNotUpdatedInTwoDays(season, d.strftime('%Y%m%d'))

print(playerIds)
print(playerIds[0][0])

for playerId in playerIds:
    processor.ProcessPlayer(playerId[0], season, d)

