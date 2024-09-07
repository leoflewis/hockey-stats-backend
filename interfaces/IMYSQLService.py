class IMYSQLService():
    def __init__(self):
        pass
    
    def Close(self):
        pass 

    def InsertSeason(self, seasonId):
        pass
        
    def InsertTeam(self, teamId, teamName):
        pass
        
    def InsertGame(self, gameId):
        pass
        
    def InsertPlayer(self, playerId, playerName, position):
        pass

    def UpsertGame(self, seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId):
        self.UpdateGameDetails(seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId)
        
    def UpdateGameDetails(self, seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId):
        print("""UPDATE Game SET Season = {}, HomeTeam = {}, AwayTeam = {}, GameDate = Date({}), 
                HomeScore = {}, AwayScore = {}, HomeXG = {}, AwayXG = {}, HomeShots = {}, 
                AwayShots = {}, GameType = {}, HomeWin = {} WHERE GameId = {}""".format(seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId))
        pass
        
    def InsertGameEvent(self, eventID, eventName, gameId, seasonId, perdiodTime, periodTimeRemaining, period, origX, orgiY, xG, playerId1, playerId2, playerId3, goalieId, shotType, eventTeam, concededTeam, homeShots, awayShots, homeXG, awayXG):
        print("INSERT INTO GameEvent(EventId, EventName, Game, Season, PeriodTime, PeriodTimeRemaining, Period, X, Y, xG, Player1, Player2, Player3, Goalie, ShotType, EventTeam, ConcededTeam, HomeShots, AwayShots, HomeXG, AwayXG) VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(eventID, eventName, gameId, seasonId, perdiodTime, periodTimeRemaining, period, origX, orgiY, xG, playerId1, playerId2, playerId3, goalieId, shotType, eventTeam, concededTeam, homeShots, awayShots, homeXG, awayXG))
        pass
    
    def GetGoalsFor(self, team, date, season):
        pass
    
    def GetGoalsAgainst(self, team, date, season):
        pass
    
    def GetShotsFor(self, team, date, season):
        pass
    
    def GetShotsAgainst(self, team, date, season):
        pass
    
    def GetFenwickFor(self, team, date, season):
        pass

    def GetFenwickAgainst(self, team, date, season):
        pass
    
    def GetXGFor(self, team, date, season):
        pass
    
    def GetXGAgainst(self, team, date, season):
        pass

    def InsertGameWithPrediction(self, gameId, winProbability, seasonId, date):
        pass
