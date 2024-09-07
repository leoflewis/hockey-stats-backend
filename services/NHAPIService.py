import requests
from datetime import datetime, timedelta
class NHLApi():
    def __init__(self):
        pass

    def GetTodayGames(self) -> dict:
        today = datetime.now()
        return requests.get("https://api-web.nhle.com/v1/schedule/{}".format(today)).json()

    def GetYesterdayGames(self, date: datetime = None) -> dict:
        today = datetime.now()
        yesterday = (today - timedelta(days = 1)).strftime('%Y-%m-%d') if not date else date.strftime('%Y-%m-%d')
        return requests.get("https://api-web.nhle.com/v1/score/{}".format(yesterday)).json()
    
    def GetGamePlays(self, gameId) -> dict:
        return requests.get("https://api-web.nhle.com/v1/gamecenter/{}/play-by-play".format(gameId)).json()
    
    def GetRoster(self, teamId, year) -> dict:
        return requests.get("https://api-web.nhle.com/v1/roster/" + str(teamId) + "/" + str(year)).json()
    
    def GetPlayerLanding(self, playerId) -> dict:
        return requests.get("https://api-web.nhle.com/v1/player/" + str(playerId) + "/landing").json()
    
    def GetLiveGames(self):
        return requests.get("https://api-web.nhle.com/v1/score/now").json()
    
    def GetRegularSeasonByGame(self, startDate:str = "2023-10-10", endDate: str = "2024-04-18"):
        date_format = "%Y-%m-%d"
        while datetime.strptime(startDate, date_format) < datetime.strptime(endDate, date_format):
            week = requests.get("https://api-web.nhle.com/v1/schedule/{}".format(startDate)).json()
            for gameDate in week["gameWeek"]:
                for game in gameDate["games"]:
                    yield game 
            startDate = week["nextStartDate"]
            if not endDate: endDate = week["regularSeasonEndDate"]