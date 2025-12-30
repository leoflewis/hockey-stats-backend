from interfaces.IMYSQLService import IMYSQLService
from services.NHAPIService import NHLApi
from datetime import datetime
from hockeylogic.Utils import FormatSeason, GetTeamIdFromName

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
        currentTotal = ''
        for seasonTotal in seasonTotals:
            if seasonTotal['gameTypeId'] == 2 and str(seasonTotal['season']) == season and seasonTotal['leagueAbbrev'] == 'NHL':
                currentTotal = seasonTotal
                print(currentTotal)
                print(currentTotal['teamName']['default'])
                team = GetTeamIdFromName(currentTotal['teamName']['default'])
                self.mysql.UpdateSeasonTotal(currentTotal, playerId, date, team)
        
        print('\n')


