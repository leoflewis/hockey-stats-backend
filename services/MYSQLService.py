import mysql.connector, os
from mysql.connector import Error
from interfaces.IMYSQLService import IMYSQLService

class MYSQLConnection(IMYSQLService):
    def __init__(self):
        self.db = mysql.connector.connect(
            host=os.environ.get("AZURE_MYSQL_HOST"),
            user=os.environ.get("AZURE_MYSQL_USER"),
            database=os.environ.get("AZURE_MYSQL_NAME"),
            password=os.environ.get("AZURE_MYSQL_PASSWORD"),
            port=3306
        )
        self.cursor = self.db.cursor()
    
    def Connected(self):
        return self.db.is_connected()

    def Close(self):
        self.db.close()

    def InsertSeason(self, seasonId):
        vals = (seasonId,)
        sql = "INSERT INTO Season(SeasonID) VALUES(%s)"

        try:
            self.cursor.execute(sql, vals)
            self.db.commit()
        except mysql.connector.errors.IntegrityError:
            return True
        except Exception:
            return False
        
    def InsertTeam(self, teamId, teamName):
        vals = (teamId, teamName)
        sql = "INSERT INTO Team(TeamID, TeamName) VALUES(%s, %s)"

        try:
            self.cursor.execute(sql, vals)
            self.db.commit()
            return True
        except mysql.connector.errors.IntegrityError:
            return True
        except Exception:
            return False
        
    def InsertGame(self, gameId):
        vals = (gameId, )
        sql = "INSERT INTO Game(GameId) VALUES(%s)"

        try:
            self.cursor.execute(sql, vals)
            self.db.commit()
            return True
        except mysql.connector.errors.IntegrityError:
            return True
        except Exception:
            return False
        
    def InsertPlayer(self, playerId, playerName, position):
        vals = (playerId, )
        sql = "INSERT INTO Player(PlayerId, PlayerName, Position) VALUES(%s, %s, %s)"
        try:
            self.cursor.execute(sql, vals)
            self.db.commit()
        except mysql.connector.errors.IntegrityError:
            return True
        except Exception:
            return False
        
    def UpdateGameDetails(self, seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId):
        vals = (seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId)
        sql = """UPDATE Game SET Season = %s, HomeTeam = %s, AwayTeam = %s, GameDate = Date(%s), 
                HomeScore = %s, AwayScore = %s, HomeXG = %s, AwayXG = %s, HomeShots = %s, 
                AwayShots = %s, GameType = %s, HomeWin = %s WHERE GameId = %s"""

        try:
            self.cursor.execute(sql, vals)
            self.db.commit()
            return True
        except mysql.connector.Error as E:
            return False
        
    def InsertGameEvent(self, eventID, eventName, gameId, seasonId, perdiodTime, periodTimeRemaining, period, origX, orgiY, xG, playerId1, playerId2, playerId3, goalieId, shotType, eventTeam, concededTeam, homeShots, awayShots, homeXG, awayXG):
        vals = (eventID, eventName, gameId, seasonId, perdiodTime, periodTimeRemaining, period, origX, orgiY, xG, playerId1, playerId2, playerId3, goalieId, shotType, eventTeam, concededTeam, homeShots, awayShots, homeXG, awayXG)
        sql = "INSERT INTO GameEvent(EventId, EventName, Game, Season, PeriodTime, PeriodTimeRemaining, Period, X, Y, xG, Player1, Player2, Player3, Goalie, ShotType, EventTeam, ConcededTeam, HomeShots, AwayShots, HomeXG, AwayXG) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            self.cursor.execute(sql, vals)
            self.db.commit()
            return True
        except mysql.connector.errors.IntegrityError:
            return False
        except Exception:
            return False
    
    def GetGoalsFor(self, team, date, season):
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < '%s' and EventName = 'GOAL' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        gf = self.cursor.fetchall()
        return 0 if not gf[0][0] else gf[0][0]
    
    def GetGoalsAgainst(self, team, date, season):
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < '%s' and EventName = 'GOAL' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        ga = self.cursor.fetchall()
        return 0 if not ga[0][0] else ga[0][0]  
    
    def GetShotsFor(self, team, date, season):
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < '%s' and EventName = 'SHOT' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        sf = self.cursor.fetchall()
        return 0 if not sf[0][0] else sf[0][0]
    
    def GetShotsAgainst(self, team, date, season):
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < '%s' and EventName = 'SHOT' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        sa = self.cursor.fetchall()
        return 0 if not sa[0][0] else sa[0][0]
    
    def GetFenwickFor(self, team, date, season):
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < '%s' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        ff = self.cursor.fetchall()
        return 0 if not ff[0][0] else ff[0][0]

    def GetFenwickAgainst(self, team, date, season):
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < '%s' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        fa = self.cursor.fetchall()
        return 0 if not fa[0][0] else fa[0][0]
    
    def GetXGFor(self, team, date, season):
        query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < '%s' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        xgf = self.cursor.fetchall()
        return 0 if not xgf[0][0] else xgf[0][0]
    
    def GetXGAgainst(self, team, date, season):
        query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < '%s' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.cursor.execute(query, vals)
        xga = self.cursor.fetchall()
        return 0 if not xga[0][0] else xga[0][0]

    def InsertGameWithPrediction(self, gameId, winProbability, seasonId, date):
        query = "INSERT INTO Game(GameId, HomeWinProba, Season, GameDate) VALUES(%s,%s,%s,%s);"
        vals = (gameId, winProbability, seasonId, date)
        try:
            self.cursor.execute(query, vals)
            self.db.commit()
            return True
        except mysql.connector.errors.IntegrityError:
            return False
        except Exception:
            return False

    def GetAllGames(self):
        query = "SELECT * FROM Game;"
        try:
            self.cursor.execute(query)
            games = self.cursor.fetchall()
            return games
        except Exception:
            return False
        
    def GetPlayers(self, seasonId):
        sql = "SELECT PlayerName, PlayerId FROM Player WHERE PlayerName IS NOT NULL;"
        if seasonId is not None:
            sql = "SELECT PlayerName, PlayerId FROM Player WHERE PlayerName IS NOT NULL AND season = " + str(seasonId) 
        self.cursor.execute(sql)
        response = self.cursor.fetchall()
        row_headers=[x[0] for x in self.cursor.description]
        json_data = []
        for result in response:
            json_data.append(dict(zip(row_headers,result)))
        return json_data
    
    def GetPlayerAverages(self, playerId)-> list:
        pass

    def GetGamesByTeamOrDate(self, teamId, date):
        if teamId and date:
            query = """
            SELECT Game.*, HT.TeamName as HomeTeamName, AWT.TeamName as AwayTeamName FROM Game 
                        JOIN Team HT on Game.HomeTeam = HT.TeamId
                        JOIN Team AWT on Game.AwayTeam = AWT.TeamId
            WHERE (HomeTeam = %s or AwayTeam = %s) AND GameDate = date(%s)
            ORDER BY GameDate DESC"""
            vals = (teamId, teamId, date) 
            self.cursor.execute(query, vals)
        elif teamId:
            query = """
            SELECT Game.*, HT.TeamName as HomeTeamName, AWT.TeamName as AwayTeamName FROM Game 
                        JOIN Team HT on Game.HomeTeam = HT.TeamId
                        JOIN Team AWT on Game.AwayTeam = AWT.TeamId
            WHERE HomeTeam = %s OR AwayTeam = %s
            ORDER BY GameDate DESC"""
            vals = (id, id)
            self.cursor.execute(query, vals)
        elif date:
            query = """ SELECT Game.*, HT.TeamName as HomeTeamName, AWT.TeamName as AwayTeamName FROM Game 
                        JOIN Team HT on Game.HomeTeam = HT.TeamId
                        JOIN Team AWT on Game.AwayTeam = AWT.TeamId
                        WHERE GameDate = date(%s)
                        ORDER BY GameDate DESC
            """
            vals = (date,)
            self.cursor.execute(query, vals)
        else:
            query = """
                        SELECT Game.*, HT.TeamName as HomeTeamName, AWT.TeamName as AwayTeamName FROM Game 
                        JOIN Team HT on Game.HomeTeam = HT.TeamId
                        JOIN Team AWT on Game.AwayTeam = AWT.TeamId
                        ORDER BY GameDate DESC
                        LIMIT 50 """
            self.cursor.execute(query)
        data = self.cursor.fetchall()
        row_headers = [x[0] for x in self.cursor.description]
        json_data = []
        for row in data:
            json_data.append(dict(zip(row_headers,row)))

        return json_data
