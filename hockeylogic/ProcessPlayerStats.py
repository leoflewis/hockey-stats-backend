from interfaces.IMYSQLService import IMYSQLService
from services.NHAPIService import NHLApi
from datetime import datetime
from hockeylogic.Utils import FormatSeason

class ProcessPlayerStats():
    
    def __init__(self, sql: IMYSQLService):
        self.mysql = sql
        self.nhl = NHLApi()
    
    def ProcessAllPlayers(self):
        date = datetime.now().date()
        season = FormatSeason(date)
        playerIds = self.mysql.GetPlayersNotUpdatedInTwoDays(season, date.strftime('%Y%m%d'))
        
        for playerId in playerIds:
            self.ProcessPlayer(playerId[0], season, date.strftime('%Y%m%d'))

    def ProcessPlayer(self, playerId: str, season: str, date: str):
        playerData = self.nhl.GetPlayerLanding(playerId)
        seasonTotals = playerData['seasonTotals']
        currenTotal = ''
        for seasonTotal in seasonTotals:
            
            if seasonTotal['gameTypeId'] == 2 and str(seasonTotal['season']) == season and seasonTotal['leagueAbbrev'] == 'NHL':
                currenTotal = seasonTotal
                break

        print(currenTotal)
        self.mysql.UpdateSeasonTotal(currenTotal, playerId, date)
        print('\n')


