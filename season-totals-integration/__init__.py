import datetime, requests, os
import logging
from mysql.connector import Error
import azure.functions as func
import mysql.connector


teamcodes = {"Wild": 30, "Bruins": 6, "Panthers": 13, "Devils": 1, "Islanders": 2, "Rangers": 3, "Flyers": 4, "Penguins": 5, "Sabres": 6, "Canadiens": 8, "Senators": 9,
             "Maple Leafs": 9, "Hurricanes": 12, "Lightning": 14, "Capitals": 15, "Blackhawks": 16, "Red Wings": 17, "Predators": 18, "Blues": 19, "Flames": 20, "Oilers": 22, "Canucks": 23,
             "Ducks": 24, "Stars":25, "Kings": 26, "Sharks": 28, "Blue Jackets": 29, "Jets": 52, "Coyotes": 53, "Golden Knights": 54, "Kraken": 55
            }


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        db = mysql.connector.connect(
            host="localhost",
            user=os.environ.get("sql-user"),
            database=os.environ.get("sql-db"),
            password=os.environ.get("sql-password"),
            port=3306
        )
    except Error as e:
        logging.info("Error while connecting to MySQL", e)

    if db.is_connected():
        logging.info("Connected to the database.")
        parse_data(db)

def parse_data(db):
    cursor = db.cursor()
    logging.info("Querying players.")
    sql = "SELECT * FROM Player;"
    today = datetime.datetime.now()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            id = row[0]
            logging.info("searching player stats")
            response = requests.get("https://api-web.nhle.com/v1/player/{}/landing".format(id)).json()
            if response['position'] != 'G':
                for stat in response['seasonTotals']:
                    if stat['season'] == '20232024' and stat['leagueAbbrev'] == 'NHL' and stat['gameTypeId'] == 2:
                        season = stat['season']
                        teamId = teamcodes[response['teamName']]
                        avgtoi = float(stat['avgToi'].replace(":", "."))
                        assists = stat['assists']
                        goals = stat['goals']
                        pim = stat['pim']
                        shots = stat['shots']
                        games = stat['gamesPlayed']
                        # no more hits 2023 
                        ppg = stat['powerPlayGoals']
                        ppp = stat['powerPlayPoints']
                        # no more pptoi 
                        # no more evtoi 
                        fopct = stat['faceoffWinningPctg']
                        shotpct = ['shootingPctg']
                        gwg = ['gameWinningGoals']
                        otg = ['otGoals']
                        # no more shg 
                        shp = stat['shorthandedPoints']
                        # no more shtoi 
                        # no more blocks 
                        pm = stat['plusMinus']
                        points = stat['points']
                        # no more shifts 
                        try:
                            logging.info("Attempting to update player data")
                            vals = (avgtoi, assists, goals, pim, shots, games, ppg, ppp, fopct, shotpct, gwg, otg, shp, pm, points, today, id, season, teamId)
                            update = "UPDATE INTO SeasonTotals SET AVGtoi = %s, Assists = %s, Goals= %s, PenMinutes = %s, Shots = %s, GamesPlayed = %s, PPGoals = %s, PPPoints = %s, FOPct = %s, ShotPct = %s, GWGoals = %s, OTGoals = %s, SHPoints = %s, PlusMinus = %s, Points = %s, last_updated = %s WHERE PlayerId = %s AND Season = %s and TeamId = %s"
                            cursor.execute(update, vals)
                            cursor.fetchall()
                            count = cursor.rowcount
                            if count == 0:
                                logging.info("Nothing to update. Attempting to insert")
                                vals = (season, id, teamId, avgtoi, assists, goals, pim, shots, games, ppg, ppp, fopct, shotpct, gwg, otg, shp, pm, points, today)
                                insert = "INSERT INTO SeasonTotals(Season, PlayerId, TeamId, AVGtoi, Assists, Goals, PenMinutes, Shots, GamesPlayed, PPGoals, PPPoints, FOPct, ShotPct, GWGoals, OTGoals, SHPoints, PlusMinus, Points, last_updated) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                cursor.execute(insert, vals)
                                cursor.fetchall()
                                count = cursor.rowcount
                                if count > 0:
                                    logging.info("Inserted player stats")

                            db.commit()
                            
                        except mysql.connector.errors.IntegrityError:
                            logging.info("Duplicate key")

    except Error:
        logging.info("Something went wrong.")
