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
    data = requests.get("https://api-web.nhle.com/v1/gamecenter/{}/play-by-play".format(game_id)).json()
    
    #season -> team -> game -> player -> game event
    awayName = data['awayTeam']['abbrev']
    homeName = data['homeTeam']['abbrev']
    
    awayId = data['awayTeam']['id']
    homeId = data['homeTeam']['id'] 
    gameId  = data['id']
    
    seasonId  = int(data['season'])
    date = data['gameDate']
    gametype = data['gameType']
    

    cursor = db.cursor()
    logging.info("done")
    
    vals = (seasonId,)
    sql = "INSERT INTO Season(SeasonID) VALUES(%s)"

    try:
        cursor.execute(sql, vals)
        db.commit()
        logging.info(sql)
    except mysql.connector.errors.IntegrityError:
        logging.info("Id already exists")

    vals = (gameId,)
    sql = "INSERT INTO Game(GameId) VALUES(%s)"

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
    homeShots = 0
    awayShots = 0
    homeScore = 0
    awayScore = 0

    for play in data['plays']:
        playType = play['typeDescKey']
        if(playType == 'shot-on-goal' or playType == 'missed-shot' or playType == 'goal'):
            logging.info(str(gameId))
            logging.info(playType)
            logging.info(play['sortOrder'])
            EventID = str(gameId) + str(play['sortOrder'])
            Goalie = None
            Player1 = None
            Player2 = None
            Player3 = None

            if 'goalieInNetId' in list(play['details'].keys()):
                Goalie = play['details']['goalieInNetId']

            if 'scoringPlayerId' in list(play['details'].keys()):
                Player1 = play['details']['scoringPlayerId']

            if 'shootingPlayerId' in list(play['details'].keys()):
                Player1 = play['details']['shootingPlayerId']

            if 'assist1PlayerId' in list(play['details'].keys()):
                Player2 = play['details']['assist1PlayerId']
                
            if 'assist2PlayerId' in list(play['details'].keys()):
                Player3 = play['details']['assist2PlayerId']

            for player in [Player1, Player2, Player3, Goalie]:
                if player is not None:
                    vals = (player, )
                    sql = "INSERT INTO Player(PlayerId) VALUES(%s)"
                    try:
                        cursor.execute(sql, vals)
                        db.commit()
                        logging.info(sql)
                    except mysql.connector.errors.IntegrityError:
                        logging.info("Id already exists")


            eventTeam = play['details']['eventOwnerTeamId']
            
            EventName = playType
            Game = gameId
            Season = seasonId
            PeriodTime = play['timeInPeriod']
            PeriodTimeRemaining = play['timeRemaining']
            period = play['period']
            x = int(play['details']['xCoord'])
            y = int(play['details']['yCoord'])
            origX = x
            origY = y
            xG = ''
            
            time = int(play['timeInPeriod'].replace(':', ''))

            homeTeamDefendingSide = play['homeTeamDefendingSide']
            # away team shooting at left side
            if homeTeamDefendingSide == 'left' and eventTeam == awayId:
                x = x * -1
                y = y * -1
            
            # home team shooting at left side
            if homeTeamDefendingSide == 'right' and eventTeam == homeId:
                x = x * -1
                y = y * -1



            new_angles = get_angles(x, y)
            new_distance = numpy.sqrt((y - 0)**2 + (x - 89.0)**2)
            try:
                #try to get shot type. missed shots do not have a type but can still have xG
                type = play['details']['shotType']
                if type == 'wrist':
                    new_shot = [[x, y, 0, 0, 0, 0, 0, 0, 0, 1, new_angles[0], new_angles[1], new_distance]]
                elif type == 'backhand':
                    new_shot = [[x, y, 0, 1, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                elif type == 'deflected':
                    new_shot = [[x, y, 0, 0, 1, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                elif type == 'slap':
                    new_shot = [[x, y, 0, 0, 0, 1, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                elif type == 'snap':
                    new_shot = [[x, y, 0, 0, 0, 0, 1, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
                elif type == 'tip-in':
                    new_shot = [[x, y, 0, 1, 0, 0, 0, 1, 0, 0, new_angles[0], new_angles[1], new_distance]]
                elif type == 'wrap-around':
                    new_shot = [[x, y, 0, 0, 0, 0, 0, 0, 1, 0, new_angles[0], new_angles[1], new_distance]]
                else:
                    new_shot = [[x, y, 1, 0, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]
            except:
                # in the event of no shot type given
                new_shot = [[x, y, 1, 0, 0, 0, 0, 0, 0, 0, new_angles[0], new_angles[1], new_distance]]                  
            if period == prev_period and prev_ev_team == play['details']['eventOwnerTeamId'] and prev_play in ['goal', 'shot', 'missed-shot'] and time - prev_time > 300:
                new_shot[0].insert(2, 1)
            else:
                new_shot[0].insert(2, 0)
            new_shot[0].insert(3, 0)
            new_df = pandas.DataFrame(new_shot, columns=predictors)
            pred = model.predict_proba(new_df)
            pred = round(pred[0][1], 4)
            
            xG = pred
            prev_ev_team = play['details']['eventOwnerTeamId']
            prev_period = period
            prev_play = playType
            prev_time = time
            
            concededTeam = ""

            if eventTeam == homeId:
                concededTeam = awayId
                home_xG += float(pred)
            if eventTeam == awayId:
                concededTeam = homeId
                away_xG += float(pred)
            
            if playType == 'shot':
                awayShots = int(play['details']['awaySOG']) + awayScore
                homeShots = int(play['details']['homeSOG']) + homeScore

            if playType == 'goal':
                awayScore = int(play['details']['awayScore'])
                homeScore = int(play['details']['homeScore'])


            xG = float(xG)
            vals = (EventID, EventName, Game, Season, PeriodTime, PeriodTimeRemaining, period, origX, origY, xG, Player1, Player2, Player3, Goalie, type, eventTeam, concededTeam, homeShots, awayShots, home_xG, away_xG)
            sql = "INSERT INTO GameEvent(EventId, EventName, Game, Season, PeriodTime, PeriodTimeRemaining, Period, X, Y, xG, Player1, Player2, Player3, Goalie, ShotType, EventTeam, ConcededTeam, HomeShots, AwayShots, HomeXG, AwayXG) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            try:
                cursor.execute(sql, vals)
                db.commit()
                logging.info(sql.format(vals))
            except mysql.connector.errors.IntegrityError:
                logging.info("Game Event Id {} already exists".format(EventID))
        
        

    homegoals = data['homeTeam']['score']
    awaygoals = data['awayTeam']['score']
    homeshots = data['homeTeam']['sog']
    awayshots = data['awayTeam']['sog']    
    away_xG = float(round(away_xG, 3))
    home_xG = float(round(home_xG, 3))

    home_win = 0
    if homegoals > awaygoals:
        home_win = 1
    

    vals = (seasonId, homeId, awayId, date, homegoals, awaygoals, home_xG, away_xG, homeshots, awayshots, gametype, home_win, gameId)
    sql = """UPDATE Game SET Season = %s SET HomeTeam = %s SET AwayTeam = %s SET GameDate = STR_TO_DATE(%s, '%Y-%m-%d') SET HomeScore = %s SET AwayScore = %s SET HomeXG = %s SET AwayXG = %s 
    SET HomeShots = %s SET AwayShots = %s SET GameType = %s SET HomeWin = %s WHERE GameId = %s"""
    logging.info(sql.format(vals))
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
    season = requests.get("https://api-web.nhle.com/v1/score/{}".format(yesterday)).json()
    if db.is_connected():
        logging.info("Connected to the database")

        games = season['games']
        for game in games:
            logging.info(game['gameDate'])
            logging.info(game['id'])
            game_data(game['id'], db)


        logging.info("Parsed all of yesterdays games") 