import requests
from datetime import datetime, timedelta
class NHLApi():

    def __init__(self):
        pass

    def GetTodayGames(self):
        today = datetime.now()
        return requests.get("https://api-web.nhle.com/v1/schedule/{}".format(today)).json()


    def GetYesterdayGames(self, date: datetime = None):
        today = datetime.now()
        yesterday = (today - timedelta(days = 1)).strftime('%Y-%m-%d') if not date else date.strftime('%Y-%m-%d')
        return requests.get("https://api-web.nhle.com/v1/score/{}".format(yesterday)).json()
    
    def GetGamePlays(self, gameId):
        return requests.get("https://api-web.nhle.com/v1/gamecenter/{}/play-by-play".format(gameId)).json()