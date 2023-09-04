import datetime, requests, os
import logging
from mysql.connector import Error
import azure.functions as func
import mysql.connector

##TODO deploy this function

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
            response = requests.get("https://statsapi.web.nhl.com/api/v1/people/{}?expand=person.stats&expand=stats.team&stats=yearByYear&season=20232024&site=en_nhl".format(id)).json()
            if response['people'][0]['primaryPosition']['code'] != 'G':
                for stat in response['people'][0]['stats'][0]['splits']:
                    if stat['season'] == '20222023' and stat['league']['name'] == 'National Hockey League':
                        season = stat['season']
                        teamId = stat['team']['id']
                        toi = float(stat['stat']['timeOnIce'].replace(":", "."))
                        assists = stat['stat']['assists']
                        goals = stat['stat']['goals']
                        pim = stat['stat']['pim']
                        shots = stat['stat']['shots']
                        games = stat['stat']['games']
                        hits = stat['stat']['hits']
                        ppg = stat['stat']['powerPlayGoals']
                        ppp = stat['stat']['powerPlayPoints']
                        pptoi = float(stat['stat']['powerPlayTimeOnIce'].replace(":", "."))
                        evtoi = float(stat['stat']['evenTimeOnIce'].replace(":", "."))
                        fopct = stat['stat']['faceOffPct']
                        shotpct = stat['stat']['shotPct']
                        gwg = stat['stat']['gameWinningGoals']
                        otg = stat['stat']['overTimeGoals']
                        shg = stat['stat']['shortHandedGoals']
                        shp = stat['stat']['shortHandedPoints']
                        shtoi = float(stat['stat']['shortHandedTimeOnIce'].replace(":", "."))
                        blocks = stat['stat']['blocked']
                        pm = stat['stat']['plusMinus']
                        points = stat['stat']['points']
                        shifts = stat['stat']['shifts']
                        try:
                            logging.info("Attempting to update player data")
                            vals = (toi, assists, goals, pim, shots, games, hits, ppg, ppp, pptoi, evtoi, fopct, shotpct, gwg, otg, shg, shp, shtoi, blocks, pm, points, shifts, today, id, season, teamId)
                            update = "UPDATE INTO SeasonTotals SET TOI = %s, Assists = %s, Goals= %s, PenMinutes = %s, Shots = %s, GamesPlayed = %s, Hits = %s, PPGoals = %s, PPPoints = %s, PPTOI = %s, EVTOI = %s, FOPct = %s, ShotPct = %s, GWGoals = %s, OTGoals = %s, SHGoals = %s, SHPoints = %s, SHTOI = %s, Blocks = %s, PlusMinus = %s, Points = %s, Shifts = %s, last_updated = %s WHERE PlayerId = %s AND Season = %s and TeamId = %s"
                            cursor.execute(update, vals)
                            cursor.fetchall()
                            count = cursor.rowcount
                            if count == 0:
                                logging.info("Nothing to update. Attempting to insert")
                                vals = (season, id, teamId, toi, assists, goals, pim, shots, games, hits, ppg, ppp, pptoi, evtoi, fopct, shotpct, gwg, otg, shg, shp, shtoi, blocks, pm, points, shifts, today)
                                insert = "INSERT INTO SeasonTotals(Season, PlayerId, TeamId, TOI, Assists, Goals, PenMinutes, Shots, GamesPlayed, Hits, PPGoals, PPPoints, PPTOI, EVTOI, FOPct, ShotPct, GWGoals, OTGoals, SHGoals, SHPoints, SHTOI, Blocks, PlusMinus, Points, Shifts, last_updated) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                cursor.execute(insert, vals)
                                cursor.fetchall()
                                count = cursor.rowcount
                                if count > 0:
                                    logging.info("Inserted player stats")

                            db.commit()
                            
                        except mysql.connector.errors.IntegrityError:
                            logging.info("Duiplicate key")

    except Error:
        logging.info("Something went wrong.")
