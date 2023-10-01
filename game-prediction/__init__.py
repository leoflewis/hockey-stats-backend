
import logging
from joblib import load
import mysql.connector, requests, pandas as pd, numpy, math, os
from mysql.connector import Error
from datetime import datetime, timedelta
import sklearn
import azure.functions as func


model = load('gamePrediction.joblib') 
predictors = ["homexGDiff", "awayxGdiff",  "homeShotDiff",  "awayShotDiff",  "homeFenDiff",  "awayFenDiff",  "homeGoalDiff",  "awayGoalDiff"]

def get_goals(date, home, away, cursor, season):
    home_awayxG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' and EventName = 'GOAL' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGf = cursor.fetchall()

    away_homexG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' and EventName = 'GOAL' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGf = cursor.fetchall()

    home_awayxG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' and EventName = 'GOAL' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGa = cursor.fetchall()

    away_homexG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' and EventName = 'GOAL' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGa = cursor.fetchall()
    hxgf = home_xGf[0][0]
    hxga = home_xGa[0][0]
    axgf = away_xGf[0][0]
    axga = away_xGa[0][0]
    if hxgf == None: hxgf = 0
    if hxga == None: hxga = 0
    if axgf == None: axgf = 0
    if axga == None: axga = 0
    homexGDiffToDate =  hxgf - hxga
    awayxGDiffToDate =  axgf - axga

    return homexGDiffToDate, awayxGDiffToDate

def get_shots(date, home, away, cursor, season):
    home_awayxG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' and EventName = 'SHOT' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGf = cursor.fetchall()

    away_homexG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' and EventName = 'SHOT' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGf = cursor.fetchall()

    home_awayxG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' and EventName = 'SHOT' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGa = cursor.fetchall()

    away_homexG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' and EventName = 'SHOT' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGa = cursor.fetchall()
    hxgf = home_xGf[0][0]
    hxga = home_xGa[0][0]
    axgf = away_xGf[0][0]
    axga = away_xGa[0][0]
    if hxgf == None: hxgf = 0
    if hxga == None: hxga = 0
    if axgf == None: axgf = 0
    if axga == None: axga = 0
    homexGDiffToDate =  hxgf - hxga
    awayxGDiffToDate =  axgf - axga

    return homexGDiffToDate, awayxGDiffToDate

def get_fenwick(date, home, away, cursor, season):
    home_awayxG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGf = cursor.fetchall()

    away_homexG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGf = cursor.fetchall()

    home_awayxG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGa = cursor.fetchall()

    away_homexG_query = "SELECT count(*) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGa = cursor.fetchall()
    hxgf = home_xGf[0][0]
    hxga = home_xGa[0][0]
    axgf = away_xGf[0][0]
    axga = away_xGa[0][0]
    if hxgf == None: hxgf = 0
    if hxga == None: hxga = 0
    if axgf == None: axgf = 0
    if axga == None: axga = 0
    homexGDiffToDate =  hxgf - hxga
    awayxGDiffToDate =  axgf - axga

    return homexGDiffToDate, awayxGDiffToDate

def get_xg(date, home, away, cursor, season):
    home_awayxG_query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGf = cursor.fetchall()

    away_homexG_query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE EventTeam = {} AND GameDate < '{}' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGf = cursor.fetchall()

    home_awayxG_query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' AND Season = {};".format(home, date, season)
    cursor.execute(home_awayxG_query)
    home_xGa = cursor.fetchall()

    away_homexG_query = "SELECT sum(xG) FROM GameEvent e JOIN Game on e.Game = Game.GameId WHERE ConcededTeam = {} AND GameDate < '{}' AND Season = {};".format(away, date, season)
    cursor.execute(away_homexG_query)
    away_xGa = cursor.fetchall()
    hxgf = home_xGf[0][0]
    hxga = home_xGa[0][0]
    axgf = away_xGf[0][0]
    axga = away_xGa[0][0]
    if hxgf == None: hxgf = 0
    if hxga == None: hxga = 0
    if axgf == None: axgf = 0
    if axga == None: axga = 0
    homexGDiffToDate =  hxgf - hxga
    awayxGDiffToDate =  axgf - axga

    return homexGDiffToDate, awayxGDiffToDate



def predictGames():
    db = None
    try:
        db = mysql.connector.connect(
            host=os.environ.get("AZURE_MYSQL_HOST"),
            user=os.environ.get("AZURE_MYSQL_USER"),
            database=os.environ.get("AZURE_MYSQL_NAME"),
            password=os.environ.get("AZURE_MYSQL_PASSWORD"),
            port=3306
        )
    except Error as e:
        print("Error while connecting to MySQL", e) 


    logging.info("Getting tomorrow date")
    today = datetime.now()
    tomorrow = (today + timedelta(days = 1)).strftime('%Y-%m-%d')
    response = requests.get("https://api-web.nhle.com/v1/schedule/{}".format(tomorrow))
    games = response['gameWeek'][0]
    if db.is_connected():
        
        cursor = db.cursor()
        
        try:

            for game in games:
                if game['gameType'] == 2:
                    gameId = game['id']
                    season = games['season']
                    homexGDiffToDate, awayxGDiffToDate = get_xg(tomorrow, game[7], game[8], cursor, season)
                    
                    homeShotDiffToDate, awayShotDiffToDate = get_shots(tomorrow, game[7], game[8], cursor, season)
                    
                    homeGoalDiffToDate, awayGoalDiffToDate = get_goals(tomorrow, game[7], game[8], cursor, season)
                    
                    homefenDiffToDate, awayFenDiffToDate = get_fenwick(tomorrow, game[7], game[8], cursor, season)
                    
                    stats = [[homexGDiffToDate, awayxGDiffToDate, homeShotDiffToDate, awayShotDiffToDate, homefenDiffToDate, awayFenDiffToDate, homeGoalDiffToDate, awayGoalDiffToDate]]
                    new_df = pd.DataFrame(stats, columns=predictors)
                    pred = model.predict_proba(new_df)
                    vals = (gameId, pred)
                    sql = "INSERT INTO Game(GameId, HomeWinProba) VALUES(%s, %s)"

                    try:
                        cursor.execute(sql, vals)
                        db.commit()
                        logging.info(sql)
                        logging.info("added row for game: {}".format(game[0]))
                    except mysql.connector.errors.IntegrityError:
                        logging.info("Error inserting prediction")
       

        except mysql.connector.errors.IntegrityError:
            logging.info("Error parsing games or making prediction")


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    predictGames()
    logging.info("Predicted tomorrows games")