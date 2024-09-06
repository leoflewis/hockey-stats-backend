from datetime import datetime
import mysql.connector, requests, pandas, numpy, math, os
from flask_cors import CORS, cross_origin
from mysql.connector import Error
import json
from flask import (Flask, jsonify, request)
import requests
from joblib import load
from services.MYSQLService import MYSQLConnection

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

sql = MYSQLConnection()

teamcodes = {"MIN": 30, "BOS": 6, "FLA": 13, "NJD": 1, "NYI": 2, "NYR": 3, "PHI": 4, "PIT": 5, "BUF": 6, "MTL": 8, "OTT": 9,
             "TOR": 9, "CAR": 12, "TBL": 14, "WSH": 15, "CHI": 16, "DET": 17, "NSH": 18, "STL": 19, "CGY": 20, "EDM": 22, "VAN": 23,
             "ANA": 24, "DAL":25, "LAK": 26, "SJS": 28, "CBJ": 29, "WPG": 52, "ARI": 53, "VGK": 54, "SEA": 55}

@app.route("/connect")
@cross_origin()
def connect():
    try:
        if sql.Connected():
            return jsonify({"message":"Connected to host!"})
        else: 
            return jsonify({"message":"Not able to connect."})
    except Exception as err:
        return jsonify({"message":"Something went wrong: {}".format(err)})
    
@app.route("/")
@cross_origin()
def index():
    status = "ok"
    message = {"message": "Hello!", "connection-status": status}
    return jsonify(message)

@app.route("/players")
@cross_origin()
def players():
    if sql.Connected():
        args = request.args
        season = args.get("season")
        try:
            json_data = sql.GetPlayers(season)
            message = {"data": json_data}
        except mysql.connector.Error as err:
            message = {"message": "There was an error {}".format(err)}
        return jsonify(message)
    else: 
        message = {"message": "Service unavailable"}
        return jsonify(message)
    

def format_shots(shots):
    formatted_shots = []
    for shot in shots:
        x = shot[0]
        y = shot[1]
        xG = (shot[2] * 100) + 5
        result = shot[3]
        x = (x * 6) + 600
        y = ((y * -1) * 6) + 255
        color = "Black"
        if result == "GOAL":
            color = "Aqua"
        if result == "MISSED_SHOT":
            color = "SpringGreen"

        new_shot = {"x":x, "y": y, "xG":xG, "Result":color}
        formatted_shots.append(new_shot)

    return formatted_shots
        


def calculate_z(json_data, player_data):
    avg_assists = float(json_data[0]['AVG(Assists)'])
    avg_goals = float(json_data[0]['AVG(Goals)'])
    avg_penm = float(json_data[0]['AVG(PenMinutes)'])
    avg_shots = float(json_data[0]['AVG(Shots)']) 
    avg_gp = float(json_data[0]['AVG(GamesPlayed)'])
    avg_hits = float(json_data[0]['AVG(Hits)'])
    avg_ppg = float(json_data[0]['AVG(PPGoals)'])
    avg_ppp = float(json_data[0]['AVG(PPPoints)'])
    avg_pptoi = float(json_data[0]['AVG(PPTOI)'])
    avg_evtoi = float(json_data[0]['AVG(EVTOI)'])
    avg_spct = float(json_data[0]['AVG(ShotPct)'])
    avg_gwg = float(json_data[0]['AVG(GWGoals)'])
    avg_otg = float(json_data[0]['AVG(OTGoals)'])
    avg_shg = float(json_data[0]['AVG(SHGoals)'])
    avg_shp = float(json_data[0]['AVG(SHPoints)'])
    avg_shtoi = float(json_data[0]['AVG(SHTOI)'])
    avg_blocks = float(json_data[0]['AVG(Blocks)'])
    avg_points = float(json_data[0]['AVG(Points)'])
    avg_pm = float(json_data[0]['AVG(PlusMinus)'])
    avg_shifts = float(json_data[0]['AVG(Shifts)'])

    std_assists = float(json_data[0]['stddev(Assists)'])
    std_goals = float(json_data[0]['stddev(Goals)'])
    std_penm = float(json_data[0]['stddev(PenMinutes)'])
    std_shots = float(json_data[0]['stddev(Shots)']) 
    std_gp = float(json_data[0]['stddev(GamesPlayed)'])
    std_hits = float(json_data[0]['stddev(Hits)'])
    std_ppg = float(json_data[0]['stddev(PPGoals)'])
    std_ppp = float(json_data[0]['stddev(PPPoints)'])
    std_pptoi = float(json_data[0]['stddev(PPTOI)'])
    std_evtoi = float(json_data[0]['stddev(EVTOI)'])
    std_spct = float(json_data[0]['stddev(ShotPct)'])
    std_gwg = float(json_data[0]['stddev(GWGoals)'])
    std_otg = float(json_data[0]['stddev(OTGoals)'])
    std_shg = float(json_data[0]['stddev(SHGoals)'])
    std_shp = float(json_data[0]['stddev(SHPoints)'])
    std_shtoi = float(json_data[0]['stddev(SHTOI)'])
    std_blocks = float(json_data[0]['stddev(Blocks)'])
    std_points = float(json_data[0]['stddev(Points)'])
    std_pm = float(json_data[0]['stddev(PlusMinus)'])
    std_shifts = float(json_data[0]['stddev(Shifts)'])

    assists = float(player_data[0]['Assists'])
    goals = float(player_data[0]['Goals'])
    penm = float(player_data[0]['PenMinutes'])
    shots = float(player_data[0]['Shots']) 
    gp = float(player_data[0]['GamesPlayed'])
    hits = float(player_data[0]['Hits'])
    ppg = float(player_data[0]['PPGoals'])
    ppp = float(player_data[0]['PPPoints'])
    pptoi = float(player_data[0]['PPTOI'])
    evtoi = float(player_data[0]['EVTOI'])
    spct = float(player_data[0]['ShotPct'])
    gwg = float(player_data[0]['GWGoals'])
    otg = float(player_data[0]['OTGoals'])
    shg = float(player_data[0]['SHGoals'])
    shp = float(player_data[0]['SHPoints'])
    shtoi = float(player_data[0]['SHTOI'])
    blocks = float(player_data[0]['Blocks'])
    points = float(player_data[0]['Points'])
    pm = float(player_data[0]['PlusMinus'])
    shifts = float(player_data[0]['Shifts'])

    z_assists = (assists - avg_assists) / std_assists
    z_goals = (goals - avg_goals) / std_goals
    z_pnm = (penm - avg_penm) / std_penm
    z_shots = (shots - avg_shots) / std_shots
    z_gp = (gp - avg_gp) / std_gp
    z_hits = (hits - avg_hits) / std_hits
    z_ppg = (ppg - avg_ppg) / std_ppg
    z_ppp = (ppp - avg_ppp) / std_ppp
    z_pptoi = (pptoi - avg_pptoi) / std_pptoi
    z_evtoi = (evtoi - avg_evtoi) / std_evtoi
    z_spct = (spct - avg_spct) / std_spct
    z_gwg = (gwg - avg_gwg) / std_gwg
    z_otg = (otg - avg_otg) / std_otg
    z_shg = (shg - avg_shg) / std_shg
    z_shp = (shp - avg_shp) / std_shp
    z_shtoi = (shtoi - avg_shtoi) / std_shtoi
    z_blocks = (blocks - avg_blocks) / std_blocks
    z_points = (points - avg_points) / std_points
    z_pm = (pm - avg_pm) / std_pm
    z_shifts = (shifts - avg_shifts) / std_shifts

    z_scores = {}
    z_scores["Assists"] = z_assists
    z_scores["Goals"] = z_goals
    z_scores["PIMs"] = z_pnm  * -1
    z_scores["Shots"] = z_shots
    z_scores["Games"] = z_gp
    z_scores["Hits"] = z_hits
    z_scores["PPGoals"] = z_ppg
    z_scores["PPPoints"] = z_ppp
    z_scores["PPTOI"] = z_pptoi
    z_scores["EVTOI"] = z_evtoi
    z_scores["Spct"] = z_spct
    z_scores["GWG"] = z_gwg
    z_scores["OTG"] = z_otg
    z_scores["SHG"] = z_shg
    z_scores["SHP"] = z_shp
    z_scores["SHTOI"] = z_shtoi
    z_scores["Blocks"] = z_blocks
    z_scores["Points"] = z_points
    z_scores["PM"] = z_pm
    z_scores["Shifts"] = z_shifts
    z_scores["SUM"] = z_assists + z_goals + (z_pnm * -1) + z_shots + z_gp + z_hits + z_ppg + z_ppp + z_pptoi + z_evtoi + z_spct + z_gwg + z_otg + z_shg + z_shp  + z_shtoi + z_blocks + z_points + z_pm + z_shifts
    return z_scores

@app.route("/player")
@cross_origin()
def player():
    args = request.args
    playerid = args.get("playerid")
    season = args.get("season", default="20222023")
    message = ""

    if playerid == None:
        return jsonify({"message":"Please provide the player id."})
    else:
        message = {"player":playerid, "season":season}

    if sql.Connected():
        json_data = []
        vals = (season,)
        query = "SELECT AVG(Assists), AVG(Goals), AVG(PenMinutes), AVG(Shots), AVG(GamesPlayed), AVG(Hits), AVG(PPGoals), AVG(PPPoints), AVG(PPTOI), AVG(EVTOI), AVG(FOPct), AVG(ShotPct), AVG(GWGoals), AVG(OTGoals), AVG(SHGoals), AVG(SHPoints), AVG(SHTOI), AVG(Blocks), AVG(PlusMinus), AVG(Points), AVG(Shifts), stddev(Assists), stddev(Goals), stddev(PenMinutes), stddev(Shots), stddev(GamesPlayed), stddev(Hits), stddev(PPGoals), stddev(PPPoints), stddev(PPTOI), stddev(EVTOI), stddev(FOPct), stddev(ShotPct), stddev(GWGoals), stddev(OTGoals), stddev(SHGoals), stddev(SHPoints), stddev(SHTOI), stddev(Blocks), stddev(PlusMinus), stddev(Points), stddev(Shifts) FROM SeasonTotals WHERE Season = %s;"
        try:
            cursor.execute(query, vals)
            response = cursor.fetchall()
            row_headers=[x[0] for x in cursor.description]
            
            for result in response:
                json_data.append(dict(zip(row_headers,result)))
            
            player_data = []
            sql = "SELECT sum(Assists) as Assists, sum(Goals) as Goals, sum(PenMinutes) as PenMinutes, sum(Shots) as Shots, sum(GamesPlayed) as GamesPlayed, sum(Hits) as Hits, sum(PPGoals) as PPGoals, sum(PPPoints) as PPPoints, sum(PPTOI) as PPTOI, sum(EVTOI) as EVTOI, sum(FOPct) as FOPct, sum(ShotPct) as ShotPct, sum(GWGoals) as GWGoals, sum(OTGoals) as OTGoals, sum(SHGoals) as SHGoals, sum(SHPoints) as SHPoints, sum(SHTOI) as SHTOI, sum(Blocks) as Blocks, sum(PlusMinus) as PlusMinus, sum(Points) as Points, sum(Shifts) as Shifts FROM seasontotals WHERE Season = %s and PlayerId = %s;"
            vals = (season, playerid)
            cursor.execute(sql, vals)
            response = cursor.fetchall()
            row_headers=[x[0] for x in cursor.description]
            
            for result in response:
                player_data.append(dict(zip(row_headers,result)))

            z_scores = calculate_z(json_data, player_data)


            sql = "SELECT x, y, xG, EventName FROM gameevent WHERE Season = %s and Player1 = %s AND Period != 5;"
            vals = (season, playerid)
            cursor.execute(sql, vals)
            pre_shots = cursor.fetchall()

            shots = format_shots(pre_shots)

            message = {"data": z_scores, "shots": shots}
            db.close()
        except mysql.connector.Error as err:
            message = {"message": "There was an error: {}".format(err)}

    else:
        message = "Service unavailable"

    return jsonify({"message": message})


@app.route("/goalie")
@cross_origin()
def goalie():
    args = request.args
    playerid = args.get("playerid")
    season = args.get("season", default="20222023")
    message = ""
    if playerid == None:
        return jsonify({"message":"Please provide the player id."})
    else:
        message = {"player":playerid, "season":season}

    db = mysql.connector.connect(
        host=os.environ.get("AZURE_MYSQL_HOST"),
        user=os.environ.get("AZURE_MYSQL_USER"),
        database=os.environ.get("AZURE_MYSQL_NAME"),
        password=os.environ.get("AZURE_MYSQL_PASSWORD"),
        port=3306,
        ssl_ca = "DigiCertGlobalRootCA.crt.pem"
    )
    if db.is_connected:
        json_data = []
        try:
            cursor = db.cursor()
            vals = (playerid, season)
            sql = "SELECT sum(xG) as total_xg_faced FROM gameevent WHERE Goalie = %s and Season = %s AND Period != 5;"
            cursor.execute(sql, vals)
            xG = cursor.fetchall()[0][0]

            sql = "SELECT count(EventId) FROM gameevent WHERE Goalie = %s and Season = %s AND EventName = 'GOAL' AND Period != 5;"
            cursor.execute(sql, vals)
            ga = cursor.fetchall()[0][0]

            sql = "SELECT x,y,xG,EventName FROM gameevent WHERE Goalie = %s and Season = %s AND Period != 5;"
            cursor.execute(sql, vals)
            pre_shots = cursor.fetchall()
            shots = format_shots(pre_shots)

            message = {"xG":xG, "GA":ga, "shots":shots}
            db.close()
        except mysql.connector.Error as err:
            message = {"message": "There was an error: {}".format(err)}
    else:
        message = "Service unavailable"

    return jsonify({"message": message})


@app.route("/skaters")
@cross_origin()
def skaters():
    args = request.args
    season = args.get("season", default="20222023")
    message = ""

    if season == None:
        return jsonify({"message":"Please provide the player id."})
    else:
        message = {"season":season}

    db = mysql.connector.connect(
        host=os.environ.get("AZURE_MYSQL_HOST"),
        user=os.environ.get("AZURE_MYSQL_USER"),
        database=os.environ.get("AZURE_MYSQL_NAME"),
        password=os.environ.get("AZURE_MYSQL_PASSWORD"),
        port=3306,
        ssl_ca = "DigiCertGlobalRootCA.crt.pem"
    )
    if db.is_connected:
        json_data = []
        cursor = db.cursor()
        vals = (season,season)
        sql = """SELECT xGTable.PlayerName, sum(TOI) as TOI, sum(Points) as Points, sum(Shots) as Shots, sum(Assists) as Apples, sum(Goals) as Genos, xGTable.xG, sum(PenMinutes) as PenM, sum(GamesPlayed) as GP, sum(Hits) as Hits, sum(PPGoals) as PPG, sum(PPPoints) as PPP, sum(PPTOI) as PPTOI, sum(EVTOI) as EVTOI, sum(FOPct) as FOPCT, sum(ShotPct) as SPct, sum(GWGoals) as GWG, sum(OTGoals) as OTG, sum(SHGoals) as SHG, sum(SHPoints) as SHP, sum(SHTOI) as SHTOI, sum(Blocks) as Blocks, sum(PlusMinus) as PM, sum(Shifts) as shifts  FROM seasontotals
            JOIN (SELECT sum(gameevent.xG) as xG, Player.PlayerId as PlayerId, Player.PlayerName as PlayerName
	        FROM gameevent Join Player on gameevent.Player1 = Player.PlayerId WHERE Period < 5 AND Season = %s
	        group by Player1) as xGTable on xGTable.PlayerId = seasontotals.PlayerId
            WHERE Season = %s
            Group by seasontotals.PlayerId
            Order by Points Desc;
        """
        try:
            cursor.execute(sql, vals)
            response = cursor.fetchall()
            row_headers=[x[0] for x in cursor.description]
            
            for result in response:
                try:
                    value = (result[0], float(result[1]), int(result[2]), int(result[3]), int(result[4]), int(result[5]), float(result[6]), int(result[7]), int(result[8]), int(result[9]), int(result[10]), int(result[11]), float(result[12]), float(result[13]), float(result[14]), float(result[15]), int(result[16]), int(result[17]), int(result[18]), int(result[19]), float(result[20]), int(result[21]), int(result[22]), int(result[23]))
                except:
                    value = result
                json_data.append(dict(zip(row_headers,value)))
            
            message = {"data": json_data}
            db.close()
        except mysql.connector.Error as err:
            message = {"message": "There was an error: {}".format(err)}

    else:
        message = "Service unavailable"

    return jsonify({"message": message})



@app.route("/team")
@cross_origin()
def team():
    args = request.args
    season = args.get("season", default="20222023")
    team = args.get("team")
    start = args.get("start")
    end = args.get("end")
    taken = args.get("taken", default="taken")
    message = ""

    if team == None:
        return jsonify({"message":"Please provide the team id."})

    teamId = teamcodes[team]

    db = mysql.connector.connect(
        host=os.environ.get("AZURE_MYSQL_HOST"),
        user=os.environ.get("AZURE_MYSQL_USER"),
        database=os.environ.get("AZURE_MYSQL_NAME"),
        password=os.environ.get("AZURE_MYSQL_PASSWORD"),
        port=3306,
        ssl_ca = "DigiCertGlobalRootCA.crt.pem"
    )
    if db.is_connected:
        json_data = []
        cursor = db.cursor()

        try:
            if taken == "taken": 
                if start != None and end != None:
                    vals= (teamId, season, start, end)
                    sql = "SELECT x,y,xG,EventName FROM gameevent e JOIN Game g on e.Game = g.GameId WHERE e.EventTeam = %s and e.Season = %s AND Period != 5 AND g.GameDate >= Date(%s)  AND g.GameDate <= Date(%s);"
                    cursor.execute(sql, vals)
                    pre_shots = cursor.fetchall()
                    shots = format_shots(pre_shots)
                    json_data = {"shots": shots}
                    message = {"data": json_data}
                else:
                    vals= (teamId, season)
                    sql = "SELECT x,y,xG,EventName FROM gameevent WHERE EventTeam = %s and Season = %s AND Period != 5;"
                    cursor.execute(sql, vals)
                    pre_shots = cursor.fetchall()
                    shots = format_shots(pre_shots)
                    json_data = {"shots": shots}
                    message = {"data": json_data}
            elif taken == "conceded":
                if start != None and end != None:
                    vals= (teamId, season, start, end)
                    sql = "SELECT x,y,xG,EventName FROM gameevent e JOIN Game g on e.Game = g.GameId WHERE e.ConcededTeam = %s and e.Season = %s AND Period != 5 AND g.GameDate >= Date(%s)  AND g.GameDate <= Date(%s);"
                    cursor.execute(sql, vals)
                    pre_shots = cursor.fetchall()
                    shots = format_shots(pre_shots)
                    json_data = {"shots": shots}
                    message = {"data": json_data}
                else:
                    vals= (teamId, season)
                    sql = "SELECT x,y,xG,EventName FROM gameevent WHERE ConcededTeam = %s and Season = %s AND Period != 5;"
                    cursor.execute(sql, vals)
                    pre_shots = cursor.fetchall()
                    shots = format_shots(pre_shots)
                    json_data = {"shots": shots}
                    message = {"data": json_data}
            db.close()
        except mysql.connector.Error as err:
            message = {"message": "There was an error: {}".format(err)}
    else:
        message = "Service unavailable"

    return jsonify({"message": message})


@app.route("/skaters-percent")
@cross_origin()
def skatersPercent():
    args = request.args
    season = args.get("season", default="20222023")
    player = args.get("id")
    if player == None:
        return jsonify({"message":"Please provide the player id."})


    db = mysql.connector.connect(
        host=os.environ.get("AZURE_MYSQL_HOST"),
        user=os.environ.get("AZURE_MYSQL_USER"),
        database=os.environ.get("AZURE_MYSQL_NAME"),
        password=os.environ.get("AZURE_MYSQL_PASSWORD"),
        port=3306,
        ssl_ca = "DigiCertGlobalRootCA.crt.pem"
    )
    if db.is_connected:
        json_data = []
        cursor = db.cursor()
        sql = "SELECT * FROM TotalsAsPercent WHERE PlayerId = %s AND Season = %s;"
        vals = (player, season)
        try:
            cursor.execute(sql, vals)
            response = cursor.fetchall()
            row_headers=[x[0] for x in cursor.description]
            
            for result in response:
                teamName = result[12]
                if "," in teamName:
                    teamName = teamName.split(",")[0]

                try:
                    value = (result[0], result[1], result[2], round(float(result[3] * 100), 3), round(float(result[4] * 100), 3), round(float(result[5] * 100), 3), round(float(result[6] * 100), 3), round(float(result[7] * 100), 3), round(float(result[8] * 100), 3), round(float(result[9] * 100), 3), round(float(result[10] * 100), 3), round(float(result[11] * 100), 3), teamName)
                except:
                    value = result
                json_data.append(dict(zip(row_headers,value)))
            
            message = {"data": json_data}
            db.close()
        except mysql.connector.Error as err:
            message = {"message": "There was an error: {}".format(err)}

    else:
        message = "Service unavailable"

    return jsonify({"message": message})


@app.route("/teams-stats")
@cross_origin()
def teamStats():
    args = request.args
    season = args.get("season", default="20222023")
    statType = args.get("type", default="0")
    statType = int(statType)
    db = mysql.connector.connect(
        host=os.environ.get("AZURE_MYSQL_HOST"),
        user=os.environ.get("AZURE_MYSQL_USER"),
        database=os.environ.get("AZURE_MYSQL_NAME"),
        password=os.environ.get("AZURE_MYSQL_PASSWORD"),
        port=3306,
        ssl_ca = "DigiCertGlobalRootCA.crt.pem"
    )
    message = ""
    if db.is_connected:
        json_data = []
        cursor = db.cursor()
        # xG
        if statType == 0:
            takenQuery = """
            SELECT Team.TeamName, sum(xG) as xGF FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
            WHERE Season = %s AND Period != 5 GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam desc;
            """
            concededQuery = """
            SELECT Team.TeamName, sum(xG) as xGA FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
            WHERE Season = %s AND Period != 5 GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam desc;
            """
            vals = (season,)
            try:
                cursor.execute(takenQuery, vals)
                taken = cursor.fetchall()

                cursor.execute(concededQuery, vals)
                conceded = cursor.fetchall()

                if len(taken) == len(conceded):
                    for i in range(len(taken)):
                        takenrow = taken[i]
                        concededrow = conceded[i]
                        if takenrow[0] == concededrow[0]: 
                            name = takenrow[0]
                            xGF = takenrow[1]
                            xGA = concededrow[1]
                            team = {"name": name, "x": xGF, "y": xGA}
                            json_data.append(team)
                    message = {"data": json_data}
                db.close()
            except mysql.connector.Error as err:
                message = {"message": "There was an error: {}".format(err)}


        if statType == 1:
            concededQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
            FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
            WHERE Season = %s AND PERIOD != 5 AND EventName = 'GOAL'
            GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam DESC;"""
            takenQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
            FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
            WHERE Season = %s AND PERIOD != 5 AND EventName = 'GOAL'
            GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam DESC;"""
            vals = (season,)
            try:
                cursor.execute(takenQuery, vals)
                taken = cursor.fetchall()

                cursor.execute(concededQuery, vals)
                conceded = cursor.fetchall()
                if len(taken) == len(conceded):
                    for i in range(len(taken)):
                        takenrow = taken[i]
                        concededrow = conceded[i]
                        if takenrow[0] == concededrow[0]: 
                            name = takenrow[0]
                            FF = takenrow[1]
                            FA = concededrow[1]
                            team = {"name": name, "x": FF, "y": FA}
                            json_data.append(team)
                    message = {"data": json_data}
                db.close()
            except mysql.connector.Error as err:
                message = {"message": "There was an error: {}".format(err)}

        if statType == 2:
            concededQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
            FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
            WHERE Season = %s AND PERIOD != 5 AND EventName = 'SHOT'
            GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam DESC;"""
            takenQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
            FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
            WHERE Season = %s AND PERIOD != 5 AND EventName = 'SHOT'
            GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam DESC;"""
            vals = (season,)
            try:
                cursor.execute(takenQuery, vals)
                taken = cursor.fetchall()

                cursor.execute(concededQuery, vals)
                conceded = cursor.fetchall()
                if len(taken) == len(conceded):
                    for i in range(len(taken)):
                        takenrow = taken[i]
                        concededrow = conceded[i]
                        if takenrow[0] == concededrow[0]: 
                            name = takenrow[0]
                            FF = takenrow[1]
                            FA = concededrow[1]
                            team = {"name": name, "x": FF, "y": FA}
                            json_data.append(team)
                    message = {"data": json_data}
                db.close()
            except mysql.connector.Error as err:
                message = {"message": "There was an error: {}".format(err)}

        if statType == 3:
            concededQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FA 
            FROM GameEvent JOIN Team on GameEvent.ConcededTeam = Team.TeamId 
            WHERE Season = %s AND PERIOD != 5 GROUP BY GameEvent.ConcededTeam ORDER BY GameEvent.ConcededTeam DESC;"""
            takenQuery = """SELECT Team.TeamName, count(GameEvent.EventID) as FF 
            FROM GameEvent JOIN Team on GameEvent.EventTeam = Team.TeamId 
            WHERE Season = %s AND PERIOD != 5 GROUP BY GameEvent.EventTeam ORDER BY GameEvent.EventTeam DESC;"""
            vals = (season,)
            try:
                cursor.execute(takenQuery, vals)
                taken = cursor.fetchall()

                cursor.execute(concededQuery, vals)
                conceded = cursor.fetchall()
                if len(taken) == len(conceded):
                    for i in range(len(taken)):
                        takenrow = taken[i]
                        concededrow = conceded[i]
                        if takenrow[0] == concededrow[0]: 
                            name = takenrow[0]
                            FF = takenrow[1]
                            FA = concededrow[1]
                            team = {"name": name, "x": FF, "y": FA}
                            json_data.append(team)
                    message = {"data": json_data}
                db.close()
            except mysql.connector.Error as err:
                message = {"message": "There was an error: {}".format(err)}

    else:
        message = "Service unavailable"

    return jsonify({"message": message})

@app.route("/teams")
@cross_origin()
def teams():
    db = mysql.connector.connect(
        host=os.environ.get("AZURE_MYSQL_HOST"),
        user=os.environ.get("AZURE_MYSQL_USER"),
        database=os.environ.get("AZURE_MYSQL_NAME"),
        password=os.environ.get("AZURE_MYSQL_PASSWORD"),
        port=3306,
        ssl_ca = "DigiCertGlobalRootCA.crt.pem"
    )
    if db.is_connected:
        json_data = []
        cursor = db.cursor()
        sql = "SELECT * FROM Team;"
        try:
            cursor.execute(sql, )
            teams = cursor.fetchall()
            row_headers = [x[0] for x in cursor.description]
            for team in teams:
                json_data.append(dict(zip(row_headers,team)))
            message = {"data": json_data}
            db.close()
        except mysql.connector.Error as err:
            message = {"message": "There was an error: {}".format(err)}

    return jsonify({"message": message})



def get_angles(x, y):
    num = math.sqrt(((89.0 - x) * (89.0 - x)) + ((y) * (y)))
    radians = numpy.arcsin(y/num)
    degrees = (radians * 180.0) / 3.14
    arr = [radians, degrees]
    return arr



model = load('xG.joblib') 
predictors = ['xC', 'yC', 'Rebound', 'Power Play', 'Type_', 'Type_BACKHAND', 'Type_DEFLECTED', 'Type_SLAP SHOT', 'Type_SNAP SHOT', 'Type_TIP-IN', 'Type_WRAP-AROUND', 'Type_WRIST SHOT', 'Angle_Radians', 'Angle_Degrees', 'Distance']




@app.route("/live-game")
@cross_origin()
def livegame():      
    args = request.args
    id = args.get("id")
    if id is None:
        return jsonify({"message":"Please provide the game id."})
    
    gameData = requests.get("https://api-web.nhle.com/v1/gamecenter/{}/play-by-play".format(id)).json()
    awayName = gameData['awayTeam']['abbrev']
    homeName = gameData['homeTeam']['abbrev']
    
    awayId = gameData['awayTeam']['id']
    homeId = gameData['homeTeam']['id'] 

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
    formattedShots = []
    homeshotsByTime = []
    awayshotsByTime = []
    homeXgByTime = []
    awayXgByTime = []
    times = []

    for play in gameData['plays']:
        playType = play['typeDescKey']
        if(playType == 'shot-on-goal' or playType == 'missed-shot' or playType == 'goal'):
            Player1 = None

            if 'scoringPlayerId' in list(play['details'].keys()):
                Player1 = play['details']['scoringPlayerId']

            if 'shootingPlayerId' in list(play['details'].keys()):
                Player1 = play['details']['shootingPlayerId']

            eventTeam = play['details']['eventOwnerTeamId']
            
            EventName = playType
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
            

            if playType == 'shot-on-goal' or playType == 'goal' and period < 5:
                if eventTeam == homeId:
                    homeShots = homeShots + 1
                elif eventTeam == awayId:
                    awayShots = awayShots + 1  
                time = float(PeriodTime.replace(":", "."))
                if period == 2:
                    time = time + 20.0
                if period == 3:
                    time = time + 40.0
                if period == 4:
                    time = time + 60.0 
                
                times.append(round(time, 2))
                homeshotsByTime.append(homeShots)
                awayshotsByTime.append(awayShots)
                homeXgByTime.append(home_xG)
                awayXgByTime.append(away_xG)
                

            xG = round(float(xG), 2)

            x = origX
            y = origY
            xG = (xG * 100) + 5

            if eventTeam == homeId and period % 2 == 0:
                asdfasd = 0

            if eventTeam == homeId:
                result = 'rgb(255, 99, 132)'
            else:
                result = 'rgb(54, 162, 235)'


            if playType == 'goal':
                result = 'Black'

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

    return jsonify({"message": message})
        


@app.route("/rosters")
@cross_origin()
def rosters():      
    args = request.args
    year = args.get("year", default=20232024)
    id = args.get("id")
    if year is None:
        return jsonify({"message":"Please list the year."})
    if id is None:
        return jsonify({"message":"Please list the team."})
    return requests.get("https://api-web.nhle.com/v1/roster/" + str(id) + "/" + str(year)).json()

@app.route("/player-proxy")
@cross_origin()
def playerproxy():      
    args = request.args

    id = args.get("id")
    if id is None:
        return jsonify({"message":"Please list the player id."})
    
    return requests.get("https://api-web.nhle.com/v1/player/" + str(id) + "/landing").json()

@app.route("/score-proxy")
@cross_origin()
def scoreproxy():      
    gameData = requests.get("https://api-web.nhle.com/v1/score/now").json()
    db = mysql.connector.connect(
        host=os.environ.get("AZURE_MYSQL_HOST"),
        user=os.environ.get("AZURE_MYSQL_USER"),
        database=os.environ.get("AZURE_MYSQL_NAME"),
        password=os.environ.get("AZURE_MYSQL_PASSWORD"),
        port=3306,
        ssl_ca = "DigiCertGlobalRootCA.crt.pem"
    )
    if db.is_connected:
        today = gameData['currentDate']
        cursor = db.cursor()
        sql = "SELECT GameId, HomeWinProba FROM Game WHERE GameDate = date(%s);"
        vals = (today,)
        try:
            cursor.execute(sql, vals)
            data = cursor.fetchall()
            if len(data) > 0:
                gameDict = dict((x, y) for x, y in data)
                if len(gameDict) == len(gameData['games']):
                    print(gameDict)
                    for game in gameData['games']:
                        if (gameDict[game['id']]) is not None:
                            homeWinProbability = float(gameDict[game['id']]) * 100
                            game['homeTeam']['winProbability'] = round(homeWinProbability, 2)
                            game['awayTeam']['winProbability'] = round((100 - homeWinProbability), 2)
                            
                            if homeWinProbability >= 50:
                                game['homeTeam']['prediction'] = 'Favored'
                                game['awayTeam']['prediction'] = 'notFavored'
                            else:
                                game['homeTeam']['prediction'] = 'notFavored'
                                game['awayTeam']['prediction'] = 'Favored'
            db.close()
            return gameData
        except mysql.connector.Error as err:
            message = {"message": "There was an error: {}".format(err)}
            db.close()
            return message
        
    else:
        message = {"message": "There was an error."}
        db.close()
    return gameData


@app.route("/games")
@cross_origin()
def games_by_team_date():
    args = request.args
    id = args.get("id")
    date = args.get("date")
    if sql.Connected():
        json_data = sql.GetGamesByTeamOrDate(id, date)
        return jsonify(json_data)
    else:
        data = "Nothing"
    return jsonify(data)
    



if __name__ == '__main__':
   app.run()






    