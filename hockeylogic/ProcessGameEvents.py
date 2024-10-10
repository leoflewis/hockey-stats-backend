from interfaces.IMYSQLService import IMYSQLService
from services.NHAPIService import NHLApi
from models.XGModel import XGModel
from datetime import datetime

class ProcessGameEvents():
    def __init__(self, sql: IMYSQLService):
        self.mysql = sql
        self.nhl = NHLApi()
        self.xg = XGModel()
    
    def ProcessSeason(self, date: datetime = None):
        season = self.nhl.GetYesterdayGames(date)
        games = season['games']
        
        for game in games:
            print(game['id'])
            self.ProcessGame(game['id'])
        
        self.mysql.Close()

    def ProcessGame(self, gameId):
        game = self.nhl.GetGamePlays(gameId)
        awayName = game['awayTeam']['abbrev']
        homeName = game['homeTeam']['abbrev']
        
        self.players = {player["playerId"]: player for player in game["rosterSpots"]}
        self.awayId = game['awayTeam']['id']
        self.homeId = game['homeTeam']['id'] 
        self.gameId  = game['id']
        
        self.seasonId  = int(game['season'])
        date = game['gameDate']
        gametype = game['gameType']

        self.mysql.InsertSeason(self.seasonId)
        self.mysql.InsertTeam(self.awayId, awayName)
        self.mysql.InsertTeam(self.homeId, homeName)
        self.mysql.InsertGame(gameId)

        self.home_xG = 0
        self.away_xG = 0
        self.prev_play = None
        self.prev_period = 0
        self.prev_ev_team = 0
        self.prev_time = 0
        self.homeShots = 0
        self.awayShots = 0

        for play in game['plays']:
            playType = play['typeDescKey']
            if(playType == 'shot-on-goal' or playType == 'missed-shot' or playType == 'goal'):
                self.ProcessPlay(play, gameId)
        
        homegoals = game['homeTeam']['score']
        awaygoals = game['awayTeam']['score']

        homeshots = self.homeShots
        if 'sog' in game['homeTeam'].keys():
            homeshots = game['homeTeam']['sog']

        awayshots = self.awayShots
        if 'sog'in game['awayTeam'].keys():
            awayshots = game['awayTeam']['sog']    
        
        away_xG = float(round(self.away_xG, 3))
        home_xG = float(round(self.home_xG, 3))

        homeWin = 0
        if homegoals > awaygoals:
            homeWin = 1
        self.mysql.UpsertGame(self.seasonId, self.homeId, self.awayId, date, homegoals, awaygoals, home_xG, away_xG, homeshots, awayshots, gametype, homeWin, gameId)

    def ProcessPlay(self, play, gameId):
        EventID = str(gameId) + str(play['sortOrder'])
        Goalie, Player1, Player2, Player3 = self.GetPlayers(play['details'])

        for player in [Player1, Player2, Player3, Goalie]:
            if player is not None:
                playerDetails = self.players[player]
                playername = playerDetails["firstName"]["default"] + " " + playerDetails["lastName"]["default"]
                position = playerDetails["positionCode"]
                self.mysql.InsertPlayer(player, playername, position)

        eventTeam = play['details']['eventOwnerTeamId']
        EventName = play['typeDescKey']
        PeriodTime = play['timeInPeriod']
        PeriodTimeRemaining = play['timeRemaining']
        period = play['periodDescriptor']['number']
        try:
            x = int(play['details']['xCoord'])
            y = int(play['details']['yCoord'])
        except: 
            x = 0
            y = 0
        origX = x
        origY = y
        
        time = int(play['timeInPeriod'].replace(':', ''))

        homeTeamDefendingSide = play['homeTeamDefendingSide']
        # away team shooting at left side
        if homeTeamDefendingSide == 'left' and eventTeam == self.awayId:
            x = x * -1
            y = y * -1
        
        # home team shooting at left side
        if homeTeamDefendingSide == 'right' and eventTeam == self.homeId:
            x = x * -1
            y = y * -1
        if 'shotType' in play['details'].keys():
            shotype = play['details']['shotType']
        else:
            shotype = ''

        if period == self.prev_period and self.prev_ev_team == play['details']['eventOwnerTeamId'] and self.prev_play in ['goal', 'shot', 'missed-shot'] and time - self.prev_time > 300:
            rebound = True
        else:
            rebound = False
        print("\t", EventID)
        xG = self.xg.Predict((x, y, shotype, rebound))

        self.prev_ev_team = play['details']['eventOwnerTeamId']
        self.prev_period = period
        self.prev_play = EventName
        self.prev_time = time
        
        concededTeam = ""

        if eventTeam == self.homeId:
            concededTeam = self.awayId
            self.home_xG += float(xG)
        if eventTeam == self.awayId:
            concededTeam = self.homeId
            self.away_xG += float(xG)
        
        if EventName == 'shot-on-goal' or EventName == 'goal':
            if eventTeam == self.awayId:
                self.awayShots += 1
            if eventTeam == self.homeId:
                self.homeShots += 1

        if EventName == 'shot-on-goal': EventName = 'SHOT'
        if EventName == 'goal': EventName = 'GOAL'
        if EventName == 'missed-shot': EventName = 'MISSED_SHOT'
        self.mysql.InsertGameEvent(EventID, EventName, gameId, self.seasonId, PeriodTime, PeriodTimeRemaining, period, origX, origY, xG, Player1, Player2, Player3, Goalie, shotype, eventTeam, concededTeam, self.homeShots, self.awayShots, round(self.home_xG, 2), round(self.away_xG, 2))

    def ProcessGameForAPI(self, gameId):
        gameData = self.nhl.GetGamePlays(gameId)
    
        awayId = gameData['awayTeam']['id']
        homeId = gameData['homeTeam']['id'] 

        home_xG = 0
        away_xG = 0
        prev_play = None
        prev_period = 0
        prev_ev_team = 0
        prev_time = 0
        homeShots = 0
        awayShots = 0
        formattedShots = []
        homeshotsByTime = []
        awayshotsByTime = []
        homeXgByTime = []
        awayXgByTime = []
        times = []

        for play in gameData['plays']:
            playType = play['typeDescKey']
            if(playType == 'shot-on-goal' or playType == 'missed-shot' or playType == 'goal'):
                eventTeam = play['details']['eventOwnerTeamId']
                PeriodTime = play['timeInPeriod']
                period = play['periodDescriptor']['number']
                try:
                    x = int(play['details']['xCoord'])
                    y = int(play['details']['yCoord'])
                except:
                    x = 0
                    y = 0
                origX = x
                origY = y
                xG = ''
                
                time = int(play['timeInPeriod'].replace(':', ''))

                homeTeamDefendingSide = play['homeTeamDefendingSide']
                # away team shooting at left side or home team shooting at left side
                if (homeTeamDefendingSide == 'left' and eventTeam == awayId) or (homeTeamDefendingSide == 'right' and eventTeam == homeId):
                    x = x * -1
                    y = y * -1

                shottype = play['details']['shotType']                 
                if period == prev_period and prev_ev_team == play['details']['eventOwnerTeamId'] and prev_play in ['goal', 'shot', 'missed-shot'] and time - prev_time > 300:
                    rebound = True
                else:
                    rebound = False
                
                xG = self.xg.Predict((x, y, shottype, rebound))
                
                prev_ev_team = play['details']['eventOwnerTeamId']
                prev_period = period
                prev_play = playType
                prev_time = time

                if eventTeam == homeId: home_xG += float(xG)
                if eventTeam == awayId: away_xG += float(xG)
                
                if playType == 'shot-on-goal' or playType == 'goal' and period < 5:
                    if eventTeam == homeId: homeShots = homeShots + 1
                    elif eventTeam == awayId: awayShots = awayShots + 1  
                    time = float(PeriodTime.replace(":", "."))
                    if period == 2: time = time + 20.0
                    if period == 3: time = time + 40.0
                    if period == 4: time = time + 60.0 
                    times.append(round(time, 2))
                    homeshotsByTime.append(homeShots)
                    awayshotsByTime.append(awayShots)
                    homeXgByTime.append(round(home_xG, 2))
                    awayXgByTime.append(round(away_xG, 2))
                    
                xG = round(float(xG), 2)

                x = origX
                y = origY
                xG = (xG * 100) + 5

                if eventTeam == homeId: result = 'rgb(255, 99, 132)'
                else: result = 'rgb(54, 162, 235)'

                if playType == 'goal': result = 'Black'

                if period % 2 == 0:
                    x = x * -1
                    y = y * -1

                x = (x * 6) + 600
                y = ((y * -1) * 6) + 255

                new_shot = {"x":x, "y": y, "xG":xG, "Result": result }
                formattedShots.append(new_shot)
        totals = {"homexG": round(home_xG, 2), "awayxg": round(away_xG, 2), "homeShots": homeShots, "awayShots": awayShots} 
        shotsByTime = {"times": times, "homeShots": homeshotsByTime, "awayShots": awayshotsByTime, "homexG": homeXgByTime, "awayxG": awayXgByTime}
        message = {"shots": formattedShots, "shotsByTime": shotsByTime, "totals": totals}
        return message
    
    def GetPlayers(self, details) -> tuple:
        if 'goalieInNetId' in list(details.keys()):
            Goalie = details['goalieInNetId']
        else:
            Goalie = None

        if 'scoringPlayerId' in list(details.keys()):
            Player1 = details['scoringPlayerId']
        else:
            Player1 = None

        if 'shootingPlayerId' in list(details.keys()):
            Player1 = details['shootingPlayerId']

        if 'assist1PlayerId' in list(details.keys()):
            Player2 = details['assist1PlayerId']
        else:
            Player2 = None
            
        if 'assist2PlayerId' in list(details.keys()):
            Player3 = details['assist2PlayerId']
        else:
            Player3 = None
        
        return Goalie, Player1, Player2, Player3
