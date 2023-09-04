import logging
import azure.functions as func
import mysql.connector, requests, pandas, numpy, math, os
from mysql.connector import Error
from joblib import load
from datetime import datetime, timedelta
import sklearn

model = load('xG.joblib') 
predictors = ['xC', 'yC', 'Rebound', 'Power Play', 'Type_', 'Type_BACKHAND', 'Type_DEFLECTED', 'Type_SLAP SHOT', 'Type_SNAP SHOT', 'Type_TIP-IN', 'Type_WRAP-AROUND', 'Type_WRIST SHOT', 'Angle_Radians', 'Angle_Degrees', 'Distance']

def get_angles(x, y):
    num = math.sqrt(((89.0 - x) * (89.0 - x)) + ((y) * (y)))
    radians = numpy.arcsin(y/num)
    degrees = (radians * 180.0) / 3.14
    arr = [radians, degrees]
    return arr

def game_data(game_id, db):
    data = requests.get("http://statsapi.web.nhl.com/api/v1/game/{}/feed/live".format(game_id)).json()

    #season -> team -> game -> player -> game event
    awayName = data['gameData']['teams']['away']['triCode']
    homeName = data['gameData']['teams']['home']['triCode']
    awayId = data['gameData']['teams']['away']['id']
    homeId = data['gameData']['teams']['home']['id'] 
    gameId  = data['gamePk']
    seasonId  = int(data['gameData']['game']['season'])
    date = data['gameData']['datetime']['dateTime']
    gametype = data['gameData']['game']['type']
    home = data['gameData']['teams']['home']['triCode']
    away = data['gameData']['teams']['away']['triCode']

    cursor = db.cursor()

    
    vals = (seasonId,)
    sql = "INSERT INTO Season(SeasonID) VALUES(%s)"

    try:
        cursor.execute(sql, vals)
        db.commit()
        logging.info(sql)
    except mysql.connector.errors.IntegrityError:
        logging.info("Id already exists")

    
    vals = (awayId, awayName)
    sql = "INSERT INTO Team(TeamID, TeamName) VALUES(%s, %s)"

    try:
        cursor.execute(sql, vals)
        db.commit()
        logging.info(sql)
    except mysql.connector.errors.IntegrityError:
        logging.info("Id already exists")


    
    vals = (homeId, homeName)
    sql = "INSERT INTO Team(TeamID, TeamName) VALUES(%s, %s)"

    try:
        cursor.execute(sql, vals)
        db.commit()
        logging.info(sql)
    except mysql.connector.errors.IntegrityError:
        logging.info("Id already exists")


    type = ''
    home_xG = 0
    away_xG = 0
    prev_play = None
    prev_period = 0
    prev_ev_team = 0
    prev_time = 0

    for play in data['liveData']['plays']['allPlays']:
        if(play['result']['eventTypeId'] == 'SHOT' or play['result']['eventTypeId'] == 'MISSED_SHOT' or play['result']['eventTypeId'] == 'GOAL'):
            players = play['players']
            for player in players:
                playerId = player['player']['id']
                playerName = player['player']['fullName']

                vals = (playerId, playerName)
                sql = "INSERT INTO Player(PlayerId, PlayerName) VALUES(%s, %s)"

                if player['playerType'] == 'Goalie' or player['playerType'] == 'Unknown':
                    goalie = player['player']['id']
                else:
                    goalie = None
            

            EventID = str(gameId) + str(play['about']['eventIdx'])
            EventName = play['result']['eventTypeId'] 
            Game = gameId
            Season = seasonId
            PeriodTime = play['about']['periodTime']
            PeriodTimeRemaining = play['about']['periodTimeRemaining']
            Period = play['about']['period']
            x = int(play['coordinates']['x'])
            y = int(play['coordinates']['y'])
            origX = x
            origY = y
            xG = ''
            Player1 = players[0]['player']['id']

            if len(players) > 1 and player['playerType'] != 'Goalie' and player['playerType'] != 'Unknown':
                Player2 = players[0]['player']['id']
            else:
                Player2 = None
                

            if len(players) > 2 and player['playerType'] != 'Goalie' and player['playerType'] != 'Unknown':
                Player3 = players[0]['player']['id']
            else:
                Player3 = None



            period = play['about']['period']
            time = int(play['about']['periodTime'].replace(':', ''))
            if play['result']['event'] == 'Goal' or play['result']['event'] == 'Shot' or play['result']['event'] == 'Missed Shot':
                if x < 0:
                    x = x * -1
                    y = y * -1
                        
                new_angles = get_angles(x, y)
                new_distance = numpy.sqrt((y - 0)**2 + (x - 89.0)**2)
                try:
                    #try to get shot type. missed shots do not have a type but can still have xG
                    type = play['result']['secondaryType']
                    if type == 'Wrist Shot':
                        new_shot = [[x, y, 0, 0, 0, 0, 0, 0, 0, 1, new_angles[0], new_angles[1], new_distance]]
                    elif type == 'Backhand':
                        new_shot = [[x, y, 0, 1, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                    elif type == 'Deflected':
                        new_shot = [[x, y, 0, 0, 1, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                    elif type == 'Slap Shot':
                        new_shot = [[x, y, 0, 0, 0, 1, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                    elif type == 'Snap Shot':
                        new_shot = [[x, y, 0, 0, 0, 0, 1, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                    elif type == 'Tip-In':
                        new_shot = [[x, y, 0, 1, 0, 0, 0, 1, 0, 0, new_angles[0], new_angles[1], new_distance]]
                    elif type == 'Wrap-around':
                        new_shot = [[x, y, 0, 0, 0, 0, 0, 0, 1, 0, new_angles[0], new_angles[1], new_distance]]
                    else:
                        new_shot = [[x, y, 1, 0, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                except:
                    # in the event of no shot type given
                    new_shot = [[x, y, 1, 0, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]                  
                if period == prev_period and prev_ev_team == play['team']['id'] and prev_play in ['Goal', 'Shot', 'Misses Shot'] and time - prev_time > 300:
                    new_shot[0].insert(2, 1)
                else:
                    new_shot[0].insert(2, 0)
                new_shot[0].insert(3, 0)
                new_df = pandas.DataFrame(new_shot, columns=predictors)
                pred = model.predict_proba(new_df)
                pred = round(pred[0][1], 4)
                
                if play['team']['triCode'] == home:
                    home_xG += pred
                if play['team']['triCode'] == away:
                    away_xG += pred

                xG = pred
                prev_ev_team = play['team']['id']
            
            prev_period = period
            prev_play = play['result']['event']
            prev_time = time
            eventTeam = play['team']['id']
            concededTeam = ""
            if eventTeam == homeId:
                concededTeam = awayId
            if eventTeam == awayId:
                concededTeam = homeId
            xG = float(xG)
            logging.info(EventID)
            vals = (EventID, EventName, Game, Season, PeriodTime, PeriodTimeRemaining, Period, origX, origY, xG, Player1, Player2, Player3, goalie, type, eventTeam, concededTeam)
            sql = "INSERT INTO GameEvent(EventId, EventName, Game, Season, PeriodTime, PeriodTimeRemaining, Period, X, Y, xG, Player1, Player2, Player3, Goalie, ShotType, EventTeam, ConcededTeam) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            try:
                cursor.execute(sql, vals)
                db.commit()
                logging.info(sql.format(vals))
            except mysql.connector.errors.IntegrityError:
                logging.info("Id already exists")

    homegoals = data['liveData']['boxscore']['teams']['home']['teamStats']['teamSkaterStats']['goals']
    awaygoals = data['liveData']['boxscore']['teams']['away']['teamStats']['teamSkaterStats']['goals']
    homeshots = data['liveData']['boxscore']['teams']['home']['teamStats']['teamSkaterStats']['shots']
    awayshots = data['liveData']['boxscore']['teams']['away']['teamStats']['teamSkaterStats']['shots']
    
    away_xG = float(round(away_xG, 3))
    home_xG = float(round(home_xG, 3))

    home_win = 0
    if homegoals > awaygoals:
        home_win = 1
    
    new_date = date.split("T")
    logging.info(new_date[0])
    vals = (gameId, seasonId, homeId, awayId, new_date[0], homegoals, awaygoals, home_xG, away_xG, homeshots, awayshots, gametype, home_win)
    sql = "INSERT INTO Game(GameId, Season, HomeTeam, AwayTeam, GameDate, HomeScore, AwayScore, HomeXG, AwayXG, HomeShots, AwayShots, GameType, HomeWin) VALUES(%s, %s, %s, %s, STR_TO_DATE(%s, '%Y-%m-%d'), %s, %s, %s, %s, %s, %s, %s)"
    logging.info("Game {} homewin {}".format(game_id, home_win))
    try:
        cursor.execute(sql, vals)
        db.commit()
        logging.info(sql)
    except mysql.connector.errors.IntegrityError:
        logging.info("Id already exists")

    

            



def main(mytimer: func.TimerRequest) -> None:
    logging.info('Starting app')
    utc_timestamp = datetime.utcnow().isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        db = mysql.connector.connect(
            host=os.environ.get("AZURE_MYSQL_HOST"),
            user=os.environ.get("AZURE_MYSQL_USER"),
            database=os.environ.get("AZURE_MYSQL_NAME"),
            password=os.environ.get("AZURE_MYSQL_PASSWORD"),
            port=3306
        )
    except Error as e:
        logging.info("Error while connecting to MySQL") 

    logging.info("Getting yesterday date")
    today = datetime.now()
    yesterday = (today - timedelta(days = 1)).strftime('%Y-%m-%d')
    logging.info("Searching for games on " + str(yesterday))
    season = requests.get("https://statsapi.web.nhl.com/api/v1/schedule?&?date={}".format(yesterday)).json()

    if db.is_connected():
        logging.info("Connected to the database")

        dates = season['dates']
        for date in dates:
            logging.info(date['date'])
            games = date['games']
            for game in games:
                game_data(game['gamePk'], db)

        logging.info("Parsed all of yesterdays games") 