from services.MYSQLService import MYSQLConnection
from services.NHAPIService import NHLApi
from models.GamePredictionModel import GamePredictionModel
from datetime import datetime

class GamePredictionEngine():
    def __init__(self):
        self.nhl = NHLApi()
        self.sql = MYSQLConnection()
        self.model = GamePredictionModel()

    def ProcessGames(self):
        response = NHLApi.GetTodayGames()
        games = response['gameWeek'][0]
        self.today = datetime.now().strftime('%Y-%m-%d')
        for game in games['games']:
            if game['gameType'] == 2:
                gameId = game['id']
                self.season = game['season']
                self.homeTeamId = game['homeTeam']['id']
                self.awayTeamId = game['awayTeam']['id']

                homexGDiffToDate, awayxGDiffToDate = self.GetXG()
                
                homeShotDiffToDate, awayShotDiffToDate = self.GetShots()
                
                homeGoalDiffToDate, awayGoalDiffToDate = self.GetGoals()
                
                homefenDiffToDate, awayFenDiffToDate = self.GetFenwick()

                result = self.model.Predict((homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate, homefenDiffToDate, awayFenDiffToDate))

                self.sql.InsertGameWithPrediction(gameId, result, self.season, self.today)

    def PredictGame(self, game):
        gameId = game[0]
        self.today = game[1]
        self.season = game[2]
        self.homeTeamId = game[3]
        self.awayTeamId = game[4]
        homexGDiffToDate, awayxGDiffToDate = self.GetXG()
                
        homeShotDiffToDate, awayShotDiffToDate = self.GetShots()
        
        homeGoalDiffToDate, awayGoalDiffToDate = self.GetGoals()
        
        homefenDiffToDate, awayFenDiffToDate = self.GetFenwick()

        result = self.model.Predict((homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate, homefenDiffToDate, awayFenDiffToDate))

        self.sql.UpdateGameWithPrediction(gameId, result)

    def ProduceParameters(self, game):
        gameId = game[0]
        self.today = game[1]
        self.season = game[2]
        self.homeTeamId = game[3]
        self.awayTeamId = game[4]
        homexGDiffToDate, awayxGDiffToDate = self.GetXG()
                
        homeShotDiffToDate, awayShotDiffToDate = self.GetShots()
        
        homeGoalDiffToDate, awayGoalDiffToDate = self.GetGoals()
        
        homefenDiffToDate, awayFenDiffToDate = self.GetFenwick()

        return homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate, homefenDiffToDate, awayFenDiffToDate


    def GetXG(self):
        homeXgFor = self.sql.GetXGFor(self.homeTeamId, self.today, self.season)
        homeXgAgainst = self.sql.GetXGAgainst(self.homeTeamId, self.today, self.season)

        awayXgFor = self.sql.GetXGFor(self.awayTeamId, self.today, self.season)
        awayXgAgainst = self.sql.GetXGAgainst(self.awayTeamId, self.today, self.season)

        return homeXgFor - homeXgAgainst, awayXgFor - awayXgAgainst

    def GetShots(self):
        homeShotsFor = self.sql.GetShotsFor(self.homeTeamId, self.today, self.season)
        homeShotsAgainst = self.sql.GetShotsAgainst(self.homeTeamId, self.today, self.season)

        awayShotsFor = self.sql.GetShotsFor(self.awayTeamId, self.today, self.season)
        awayShotsAgainst = self.sql.GetShotsAgainst(self.awayTeamId, self.today, self.season)

        return homeShotsFor - homeShotsAgainst, awayShotsFor - awayShotsAgainst

    def GetGoals(self):
        homeGoalsFor = self.sql.GetGoalsFor(self.homeTeamId, self.today, self.season)
        homeGoalsAgainst = self.sql.GetGoalsAgainst(self.homeTeamId, self.today, self.season)

        awayGoalsFor = self.sql.GetGoalsFor(self.awayTeamId, self.today, self.season)
        awayGoalsAgainst = self.sql.GetGoalsAgainst(self.awayTeamId, self.today, self.season)

        return homeGoalsFor - homeGoalsAgainst, awayGoalsFor - awayGoalsAgainst

    def GetFenwick(self):
        homeFenwickFor = self.sql.GetFenwickFor(self.homeTeamId, self.today, self.season)
        homeFenwickAgainst = self.sql.GetFenwickAgainst(self.homeTeamId, self.today, self.season)

        awayFenwickFor = self.sql.GetFenwickFor(self.awayTeamId, self.today, self.season)
        awayFenwickAgainst = self.sql.GetFenwickAgainst(self.awayTeamId, self.today, self.season)

        return homeFenwickFor - homeFenwickAgainst, awayFenwickFor - awayFenwickAgainst