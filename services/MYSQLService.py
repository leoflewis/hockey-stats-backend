import mysql.connector, os
from mysql.connector import Error
from interfaces.IMYSQLService import IMYSQLService

class MYSQLConnection(IMYSQLService):
    def __init__(self):
        pass

    def Connect(self):
        self.db = mysql.connector.connect(
            host=os.environ.get("AZURE_MYSQL_HOST"),
            user=os.environ.get("AZURE_MYSQL_USER"),
            database=os.environ.get("AZURE_MYSQL_NAME"),
            password=os.environ.get("AZURE_MYSQL_PASSWORD"),
            port=3306
        )
        self.cursor = self.db.cursor()

    def Connected(self) -> bool:
        try:
            self.Connect()
            self.Close()
            return True
        except:
            return False

    def Close(self):
        self.db.close()

    def InsertSeason(self, seasonId) -> bool:
        vals = (seasonId,)
        sql = "INSERT INTO Season(SeasonID) VALUES(%s)"
        try:
            self.Connect()
            self.cursor.execute(sql, vals)
            self.db.commit()
            self.Close()
        except mysql.connector.errors.IntegrityError:
            pass
        except Exception:
            return False
        finally:
            self.Close()
            return True
        
    def InsertTeam(self, teamId, teamName) -> bool:
        vals = (teamId, teamName)
        sql = "INSERT INTO Team(TeamID, TeamName) VALUES(%s, %s)"
        try:
            self.Connect()
            self.cursor.execute(sql, vals)
            self.db.commit()
            self.Close()
        except mysql.connector.errors.IntegrityError:
            pass
        except Exception:
            return False
        finally:
            return True
        
    def InsertGame(self, gameId) -> bool:
        vals = (gameId, )
        sql = "INSERT INTO Game(GameId) VALUES(%s)"
        try:
            self.Connect()
            self.cursor.execute(sql, vals)
            self.db.commit()
            self.Close()
        except mysql.connector.errors.IntegrityError:
            pass
        except Exception:
            return False
        finally:
            return True
        
    def InsertPlayer(self, playerId, playerName, position) -> bool:
        vals = (playerId, playerName, position)
        sql = "INSERT INTO Player(PlayerId, PlayerName, Position) VALUES(%s, %s, %s)"
        try:
            self.Connect()
            self.cursor.execute(sql, vals)
            self.db.commit()
            self.Close()
        except mysql.connector.errors.IntegrityError:
            pass
        except Exception:
            return False
        finally:
            return True
        
    def UpsertGame(self, seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId):
        vals = (seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId)
        sql = """INSERT INTO Game(Season, HomeTeam, AwayTeam, GameDate, HomeScore, AwayScore, HomeXG, AwayXG, HomeShots, AwayShots, GameType, HomeWin, GameID) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            self.Connect()
            self.cursor.execute(sql, vals)
            self.db.commit()
            self.Close()
        except mysql.connector.errors.IntegrityError:
            return self.UpdateGameDetails(seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId)
        return True        
        
    def UpdateGameDetails(self, seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId) -> bool:
        vals = (seasonId, homeTeamId, awayTeamId, date, homeScore, awayScore, homeXG, awayXG, homeShots, awayShots, gameType, homeWin, gameId)
        sql = """UPDATE Game SET Season = %s, HomeTeam = %s, AwayTeam = %s, GameDate = Date(%s), 
                HomeScore = %s, AwayScore = %s, HomeXG = %s, AwayXG = %s, HomeShots = %s, 
                AwayShots = %s, GameType = %s, HomeWin = %s WHERE GameId = %s"""
        self.Connect()
        self.cursor.execute(sql, vals)
        self.db.commit()
        self.Close()
        return True
        
    def InsertGameEvent(self, eventID, eventName, gameId, seasonId, perdiodTime, periodTimeRemaining, period, origX, orgiY, xG, playerId1, playerId2, playerId3, goalieId, shotType, eventTeam, concededTeam, homeShots, awayShots, homeXG, awayXG) -> bool:
        vals = (eventID, eventName, gameId, seasonId, perdiodTime, periodTimeRemaining, period, origX, orgiY, xG, playerId1, playerId2, playerId3, goalieId, shotType, eventTeam, concededTeam, homeShots, awayShots, homeXG, awayXG)
        sql = "INSERT INTO GameEvent(EventId, EventName, Game, Season, PeriodTime, PeriodTimeRemaining, Period, X, Y, xG, Player1, Player2, Player3, Goalie, ShotType, EventTeam, ConcededTeam, HomeShots, AwayShots, HomeXG, AwayXG) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            self.Connect()
            self.cursor.execute(sql, vals)
            self.db.commit()
        except mysql.connector.errors.IntegrityError:
            pass
        finally:
            self.Close()
            return True
    
    def GetGoalsFor(self, team, date, season) -> int:
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < %s and EventName = 'GOAL' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        gf = self.cursor.fetchall()
        self.Close()
        return 0 if not gf[0][0] else gf[0][0]
    
    def GetGoalsAgainst(self, team, date, season) -> int:
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < %s and EventName = 'GOAL' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        ga = self.cursor.fetchall()
        self.Close()
        return 0 if not ga[0][0] else ga[0][0]  
    
    def GetShotsFor(self, team, date, season) -> int:
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < %s and EventName = 'SHOT' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        sf = self.cursor.fetchall()
        self.Close()
        return 0 if not sf[0][0] else sf[0][0]
    
    def GetShotsAgainst(self, team, date, season) -> int:
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < %s and EventName = 'SHOT' AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        sa = self.cursor.fetchall()
        self.Close()
        return 0 if not sa[0][0] else sa[0][0]
    
    def GetFenwickFor(self, team, date, season) -> int:
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < %s AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        ff = self.cursor.fetchall()
        self.Close()
        return 0 if not ff[0][0] else ff[0][0]

    def GetFenwickAgainst(self, team, date, season) -> int:
        query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < %s AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        fa = self.cursor.fetchall()
        self.Close()
        return 0 if not fa[0][0] else fa[0][0]
    
    def GetXGFor(self, team, date, season)-> float:
        query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = %s AND GameDate < %s AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        xgf = self.cursor.fetchall()
        self.Close()
        return float(0) if not xgf[0][0] else float(xgf[0][0])
    
    def GetXGAgainst(self, team, date, season) -> float:
        query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = %s AND GameDate < %s AND Game.Season = %s AND Period != 5;"
        vals = (team, date, season)
        self.Connect()
        self.cursor.execute(query, vals)
        xga = self.cursor.fetchall()
        self.Close()
        return float(0) if not xga[0][0] else float(xga[0][0])

    def InsertGameWithPrediction(self, gameId, winProbability, seasonId, date) -> bool:
        query = "INSERT INTO Game(GameId, HomeWinProba, Season, GameDate) VALUES(%s,%s,%s,%s);"
        vals = (gameId, winProbability, seasonId, date)
        try:
            self.Connect()
            self.cursor.execute(query, vals)
            self.db.commit()
        except mysql.connector.errors.IntegrityError:
            pass
        except Exception:
            self.Close()
            return False
        finally:
            self.Close()
            return True

    def GetAllGames(self)-> list:
        query = "SELECT * FROM Game;"
        self.Connect()
        self.cursor.execute(query)
        games = self.cursor.fetchall()
        self.Close()
        return games
    
    def GetAllTeams(self)-> list:
        query = "SELECT * FROM Team;"
        self.Connect()
        self.cursor.execute(query)
        teams = self.cursor.fetchall()
        json_data = []
        row_headers = [x[0] for x in self.cursor.description]
        for team in teams:
            json_data.append(dict(zip(row_headers,team)))
        self.Close()
        return json_data
        
    def GetPlayers(self, seasonId) -> dict:
        sql = "SELECT PlayerName, PlayerId FROM Player WHERE PlayerName IS NOT NULL;"
        if seasonId is not None:
            sql = "SELECT PlayerName, PlayerId FROM Player WHERE PlayerName IS NOT NULL AND season = " + str(seasonId) 
        self.Connect()
        self.cursor.execute(sql)
        response = self.cursor.fetchall()
        row_headers=[x[0] for x in self.cursor.description]
        json_data = []
        for result in response:
            json_data.append(dict(zip(row_headers,result)))
        self.Close()
        return json_data
    
    def GetAveragesAndStDevForSeason(self, seasonId) -> list:
        query = "SELECT AVG(Assists), AVG(Goals), AVG(PenMinutes), AVG(Shots), AVG(GamesPlayed), AVG(Hits), AVG(PPGoals), AVG(PPPoints), AVG(PPTOI), AVG(EVTOI), AVG(FOPct), AVG(ShotPct), AVG(GWGoals), AVG(OTGoals), AVG(SHGoals), AVG(SHPoints), AVG(SHTOI), AVG(Blocks), AVG(PlusMinus), AVG(Points), AVG(Shifts), stddev(Assists), stddev(Goals), stddev(PenMinutes), stddev(Shots), stddev(GamesPlayed), stddev(Hits), stddev(PPGoals), stddev(PPPoints), stddev(PPTOI), stddev(EVTOI), stddev(FOPct), stddev(ShotPct), stddev(GWGoals), stddev(OTGoals), stddev(SHGoals), stddev(SHPoints), stddev(SHTOI), stddev(Blocks), stddev(PlusMinus), stddev(Points), stddev(Shifts) FROM SeasonTotals WHERE Season = %s;"
        vals = (seasonId,)
        self.Connect()
        self.cursor.execute(query, vals)
        response = self.cursor.fetchall()
        row_headers=[x[0] for x in self.cursor.description]
        json_data = []
        for result in response:
            json_data.append(dict(zip(row_headers,result)))
        self.Close()
        return json_data
    
    def GetPlayerTotalsForSeason(self, playerId, seasonId) -> list:
        query = "SELECT sum(Assists) as Assists, sum(Goals) as Goals, sum(PenMinutes) as PenMinutes, sum(Shots) as Shots, sum(GamesPlayed) as GamesPlayed, sum(Hits) as Hits, sum(PPGoals) as PPGoals, sum(PPPoints) as PPPoints, sum(PPTOI) as PPTOI, sum(EVTOI) as EVTOI, sum(FOPct) as FOPct, sum(ShotPct) as ShotPct, sum(GWGoals) as GWGoals, sum(OTGoals) as OTGoals, sum(SHGoals) as SHGoals, sum(SHPoints) as SHPoints, sum(SHTOI) as SHTOI, sum(Blocks) as Blocks, sum(PlusMinus) as PlusMinus, sum(Points) as Points, sum(Shifts) as Shifts FROM seasontotals WHERE Season = %s and PlayerId = %s;"
        vals = (seasonId, playerId)
        self.Connect()
        self.cursor.execute(query, vals)
        response = self.cursor.fetchall()
        row_headers=[x[0] for x in self.cursor.description]
        player_data = []
        for result in response:
            player_data.append(dict(zip(row_headers,result)))
        self.Close()
        return player_data 
    
    def GetPlayerShotsForSeason(self, playerId, seasonId) -> list:
        query = "SELECT x, y, xG, EventName FROM gameevent WHERE Season = %s and Player1 = %s AND Period != 5;"
        vals = (seasonId, playerId)
        self.Connect()
        self.cursor.execute(query, vals)
        pre_shots = self.cursor.fetchall()
        self.Close()
        return pre_shots
    
    def GetXGFacedByGoalie(self, playerId, seasonId) -> float:
        vals = (playerId, seasonId)
        query = "SELECT sum(xG) as total_xg_faced FROM gameevent WHERE Goalie = %s and Season = %s AND Period != 5;"
        self.Connect()
        self.cursor.execute(query, vals)
        xG = self.cursor.fetchall()[0][0]
        self.Close()
        return float(xG)

    def GetGoalsAllowedByGoalie(self, playerId, seasonId) -> int:
        vals = (playerId, seasonId)
        query = "SELECT count(EventId) FROM gameevent WHERE Goalie = %s and Season = %s AND EventName = 'GOAL' AND Period != 5;"
        self.Connect()
        self.cursor.execute(query, vals)
        ga = self.cursor.fetchall()[0][0]
        self.Close()
        return int(ga)

    def GetCoordinatesOfShotsForGoalie(self, playerId, seasonId) -> list: 
        vals = (playerId, seasonId)
        query = "SELECT x,y,xG,EventName FROM gameevent WHERE Goalie = %s and Season = %s AND Period != 5;"
        self.Connect()
        self.cursor.execute(query, vals)
        pre_shots = self.cursor.fetchall()
        self.Close()
        return pre_shots
        

    def GetGamesByTeamOrDate(self, teamId, date) -> dict:
        self.Connect()
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
            vals = (teamId, teamId)
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
        self.Close()
        return json_data
    
    def GetSkatersTableData(self, seasonId) -> dict:
        json_data = []
        vals = (seasonId,seasonId)
        query = """SELECT xGTable.PlayerName, sum(TOI) as TOI, sum(Points) as Points, sum(Shots) as Shots, sum(Assists) as Apples, sum(Goals) as Genos, xGTable.xG, sum(PenMinutes) as PenM, sum(GamesPlayed) as GP, sum(Hits) as Hits, sum(PPGoals) as PPG, sum(PPPoints) as PPP, sum(PPTOI) as PPTOI, sum(EVTOI) as EVTOI, sum(FOPct) as FOPCT, sum(ShotPct) as SPct, sum(GWGoals) as GWG, sum(OTGoals) as OTG, sum(SHGoals) as SHG, sum(SHPoints) as SHP, sum(SHTOI) as SHTOI, sum(Blocks) as Blocks, sum(PlusMinus) as PM, sum(Shifts) as shifts  FROM seasontotals
            JOIN (SELECT sum(gameevent.xG) as xG, Player.PlayerId as PlayerId, Player.PlayerName as PlayerName
	        FROM gameevent Join Player on gameevent.Player1 = Player.PlayerId WHERE Period < 5 AND Season = %s
	        group by Player1) as xGTable on xGTable.PlayerId = seasontotals.PlayerId
            WHERE Season = %s
            Group by seasontotals.PlayerId
            Order by Points Desc;
        """
        self.Connect()
        self.cursor.execute(query, vals)
        response = self.cursor.fetchall()
        row_headers=[x[0] for x in self.cursor.description]
        
        for result in response:
            try:
                value = (result[0], float(result[1]), int(result[2]), int(result[3]), int(result[4]), int(result[5]), float(result[6]), int(result[7]), int(result[8]), int(result[9]), int(result[10]), int(result[11]), float(result[12]), float(result[13]), float(result[14]), float(result[15]), int(result[16]), int(result[17]), int(result[18]), int(result[19]), float(result[20]), int(result[21]), int(result[22]), int(result[23]))
            except:
                value = result
            json_data.append(dict(zip(row_headers,value)))
        self.Close()
        return json_data
    
    def GetShotsForTeamInRange(self, teamId, season, start, end) -> list:
        vals = (teamId, season, start, end)
        query = "SELECT x,y,xG,EventName FROM gameevent e JOIN Game g on e.Game = g.GameId WHERE e.EventTeam = %s and e.Season = %s AND Period != 5 AND g.GameDate >= Date(%s)  AND g.GameDate <= Date(%s);"
        self.Connect()
        self.cursor.execute(query, vals)
        data = self.cursor.fetchall()
        self.Close()
        return data 
    
    def GetShotsForTeam(self, teamId, season) -> list:
        vals = (teamId, season)
        query = "SELECT x,y,xG,EventName FROM gameevent WHERE EventTeam = %s and Season = %s AND Period != 5;"
        self.Connect()
        self.cursor.execute(query, vals)
        data = self.cursor.fetchall()
        self.Close()
        return data 
    
    def GetShotsAgainstTeamInRange(self, teamId, season, start, end) -> list:
        vals = (teamId, season, start, end)
        query = "SELECT x,y,xG,EventName FROM gameevent e JOIN Game g on e.Game = g.GameId WHERE e.ConcededTeam = %s and e.Season = %s AND Period != 5 AND g.GameDate >= Date(%s)  AND g.GameDate <= Date(%s);"
        self.Connect()
        self.cursor.execute(query, vals)
        data = self.cursor.fetchall()
        self.Close()
        return data 

    def GetShotsAgainstTeam(self, teamId, season) -> list:
        vals = (teamId, season)
        query = "SELECT x,y,xG,EventName FROM gameevent WHERE ConcededTeam = %s and Season = %s AND Period != 5;"
        self.Connect()
        self.cursor.execute(query, vals)
        data = self.cursor.fetchall()
        self.Close()
        return data 
    
    def GetSkatersPercent(self, playerId, seasonId) -> list:
        json_data = []
        sql = "SELECT * FROM TotalsAsPercent WHERE PlayerId = %s AND Season = %s;"
        vals = (playerId, seasonId)
        self.Connect()
        self.cursor.execute(sql, vals)
        response = self.cursor.fetchall()
        row_headers = [x[0] for x in self.cursor.description]
        for result in response:
            teamName = result[12]
            if "," in teamName: teamName = teamName.split(",")[0]
            try:
                value = (result[0], result[1], result[2], round(float(result[3] * 100), 3), round(float(result[4] * 100), 3), round(float(result[5] * 100), 3), round(float(result[6] * 100), 3), round(float(result[7] * 100), 3), round(float(result[8] * 100), 3), round(float(result[9] * 100), 3), round(float(result[10] * 100), 3), round(float(result[11] * 100), 3), teamName)
            except:
                value = result
            json_data.append(dict(zip(row_headers,value)))
        self.Close()
        return json_data
    

    def GetXGFandXGAForAllTeamsSeason(self, seasonId):
        takenQuery = """
        SELECT Team.TeamName, sum(xG) as xGF FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
        WHERE Season = %s AND Period != 5 GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam desc;
        """
        concededQuery = """
        SELECT Team.TeamName, sum(xG) as xGA FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
        WHERE Season = %s AND Period != 5 GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam desc;
        """
        vals = (seasonId,)
        self.Connect()
        self.cursor.execute(takenQuery, vals)
        taken = self.cursor.fetchall()

        self.cursor.execute(concededQuery, vals)
        conceded = self.cursor.fetchall()

        json_data = []
        if len(taken) != len(conceded): raise Exception("SQL Data is misaligned")
        for i in range(len(taken)):
            takenrow = taken[i]
            concededrow = conceded[i]
            if takenrow[0] == concededrow[0]: 
                name = takenrow[0]
                xGF = takenrow[1]
                xGA = concededrow[1]
                team = {"name": name, "x": xGF, "y": xGA}
                json_data.append(team)
        self.Close()
        return json_data
    
    def GetGFandGAForAllTeamsSeason(self, seasonId):
        concededQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
        FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
        WHERE Season = %s AND PERIOD != 5 AND EventName = 'GOAL'
        GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam DESC;"""
        takenQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
        FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
        WHERE Season = %s AND PERIOD != 5 AND EventName = 'GOAL'
        GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam DESC;"""
        vals = (seasonId,)
        self.Connect()
        self.cursor.execute(takenQuery, vals)
        taken = self.cursor.fetchall()

        self.cursor.execute(concededQuery, vals)
        conceded = self.cursor.fetchall()
        
        if len(taken) != len(conceded): raise Exception("SQL Data is misaligned")

        json_data = []

        for i in range(len(taken)):
            takenrow = taken[i]
            concededrow = conceded[i]
            if takenrow[0] == concededrow[0]: 
                name = takenrow[0]
                GF = takenrow[1]
                GA = concededrow[1]
                team = {"name": name, "x": GF, "y": GA}
                json_data.append(team)
        self.Close()
        return json_data
    
    def GetSFandSAForAllTeamsSeason(self, seasonId):
        concededQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
        FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
        WHERE Season = %s AND PERIOD != 5 AND EventName = 'SHOT'
        GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam DESC;"""
        takenQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
        FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
        WHERE Season = %s AND PERIOD != 5 AND EventName = 'SHOT'
        GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam DESC;"""
        vals = (seasonId,)
        self.Connect()
        self.cursor.execute(takenQuery, vals)
        taken = self.cursor.fetchall()

        self.cursor.execute(concededQuery, vals)
        conceded = self.cursor.fetchall()
        if len(taken) != len(conceded): raise Exception("SQL Data is misaligned")
        json_data = []
        for i in range(len(taken)):
            takenrow = taken[i]
            concededrow = conceded[i]
            if takenrow[0] == concededrow[0]: 
                name = takenrow[0]
                SF = takenrow[1]
                SA = concededrow[1]
                team = {"name": name, "x": SF, "y": SA}
                json_data.append(team)
        self.Close()
        return json_data

    def GetFFandFAForAllTeamsSeason(self, seasonId):
        concededQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FA 
        FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
        WHERE Season = %s AND PERIOD != 5 GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam DESC;"""
        takenQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
        FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
        WHERE Season = %s AND PERIOD != 5 GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam DESC;"""
        vals = (seasonId,)
        self.Connect()
        self.cursor.execute(takenQuery, vals)
        taken = self.cursor.fetchall()

        self.cursor.execute(concededQuery, vals)
        conceded = self.cursor.fetchall()
        if len(taken) != len(conceded): raise Exception("SQL Data is misaligned")
        json_data = []
        for i in range(len(taken)):
            takenrow = taken[i]
            concededrow = conceded[i]
            if takenrow[0] == concededrow[0]: 
                name = takenrow[0]
                FF = takenrow[1]
                FA = concededrow[1]
                team = {"name": name, "x": FF, "y": FA}
                json_data.append(team)
        self.Close()
        return json_data
    
    def GetGamesAndWinProbability(self, date):
        sql = "SELECT GameId, HomeWinProba FROM Game WHERE GameDate = date(%s);"
        vals = (date,)
        self.Connect()
        self.cursor.execute(sql, vals)
        data = self.cursor.fetchall()
        self.Close()
        return data  
    
    def GetAllGames202324(self):
        query = "SELECT GameId, GameDate FROM Game WHERE Season = 20232024 ORDER BY GameId DESC;"
        self.Connect()
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        self.Close()
        return data 

    def GetUnPredictedGames20232024(self):
        query = "SELECT GameId, GameDate, season, HomeTeam, AwayTeam FROM Game WHERE Season = 20232024 AND HomeWinProba IS NULL ORDER BY GameId;"
        self.Connect()
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        self.Close()
        return data
    
    def UpdateGameWithPrediction(self, gameId, prediction):
        query = "UPDATE Game SET HomeWinProba = %s WHERE GameId = %s;"
        vals = (prediction, gameId)
        try:
            self.Connect()
            self.cursor.execute(query, vals)
            self.db.commit()
        except Exception as error:
            print(error)
            print(type(error))
        self.Close()

    def GetAllGamesInBatches(self):
        pageSize = 50
        pageNum = 0
        query = "SELECT GameId, GameDate, season, HomeTeam, AwayTeam, HomeWin FROM Game WHERE Season IS NOT NULL Order By GameId LIMIT %s OFFSET %s;"
        moreData = True
        self.Connect()
        while moreData:
            vals = (pageSize, pageSize * pageNum)
            self.cursor.execute(query, vals)
            data = self.cursor.fetchall()
            if len(data) == 0: moreData = False
            for row in data: yield row
            pageNum += 1
        self.Close()
        
    def GetAllEventsForAGame(self, gameId):
        query = """SELECT Period, PeriodTime, HomeShots, AwayShots, HomeXG, AwayXG FROM GameEvent
        WHERE Game = %s
        ORDER BY Period ASC, PeriodTime;"""
        vals = (gameId,)
        try:
            self.Connect()
            self.cursor.execute(query, vals)
            data = self.cursor.fetchall()
        except Exception as error:
            print(error)
            print(type(error))
        self.Close()
        return data
