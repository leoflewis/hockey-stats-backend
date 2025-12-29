import sys, os
os.chdir("..")
sys.path.append(os.getcwd())

from flask_cors import CORS, cross_origin
from flask import (Flask, jsonify, request)
from services.MYSQLService import MYSQLConnection
from services.NHAPIService import NHLApi
from models.XGModel import XGModel
from hockeylogic.ProcessGameEvents import ProcessGameEvents

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

sql = MYSQLConnection()
xgModel = XGModel()
nhl = NHLApi()
processor = ProcessGameEvents(sql=None)

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
        return jsonify({"message":"Something went wrong: {}".format(type(err))})
    
@app.route("/")
@cross_origin()
def index():
    message = {"message": "This is in the index for Leo Lewis' Hockey Stats API.",
               "endpoints": {
                   "/players": {
                       "parameters": ["season"],
                        "returns": "List of NHL Players and Ids"
                    },
                    "/player" : {
                        "parameters": ["playerid", "season?"],
                        "returns": "Single player NHL data for given season."
                    },
                    "/goalie" : {
                        "parameters": ["playerid", "season?"],
                        "returns": "Single goalie NHL data for given season."
                    },
                    "/skaters" : {
                        "parameters": ["season?"],
                        "returns": "Tabular data for all non goalies in given NHL season."
                    },
                    "/team" : {
                        "parameters": ["season?", "team", "start?", "end?"],
                        "returns": "List of shot data for the given team and season in the given range if provided."
                    },
                    "/skaters-percent" : {
                        "parameters": ["season?", "id"],
                        "returns": "NHL data for provided player and season represented in percentile form."
                    },
                    "/teams-stats" : {
                        "parameters": ["season?", "type?"],
                        "returns": "Data for all NHL teams in the given season and stat type.",
                        "options": {
                            "type":[
                                {0:"xG"},
                                {1:"Goals"},
                                {2:"Shots"},
                                {3:"Fenwick"}
                            ]
                        }
                    },
                    "/teams" : {
                        "parameters": [],
                        "returns": "List all NHL teams there are records for."
                    },
                    "/live-game" : {
                        "parameters": ["id"],
                        "returns": "Returns data for a specific game. Performs XG calculation."
                    },
                    "/rosters" : {
                        "parameters": ["id", "year?"],
                        "returns": "Returns data for the given team and year."
                    },
                    "/player-proxy" : {
                        "parameters": ["id"],
                        "returns": "Returns bio data for the given player."
                    },
                    "/score-proxy" : {
                        "parameters": [],
                        "returns": "Returns live NHL games with win probability if available."
                    },
                    "/games" : {
                        "parameters": ["id?", "date?"],
                        "returns": "Returns finalized NHL games for given team or date, with win probability if available."
                    },
                    "/game-shots" : {
                        "parameters": ["gameId"],
                        "returns": "Returns home shots, away shots, home xg and away xg at the moment it happened."
                    }
                }
            }
    return jsonify(message)

@app.route("/players")
@cross_origin()
def players():
    if not sql.Connected(): return jsonify({"message": "Service unavailable"})
    args = request.args
    season = args.get("season")
    try:
        json_data = sql.GetPlayers(season)      
        return jsonify({"data": json_data})
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

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
    avg_ppg = float(json_data[0]['AVG(PPGoals)'])
    avg_ppp = float(json_data[0]['AVG(PPPoints)'])
    avg_spct = float(json_data[0]['AVG(ShotPct)'])
    avg_gwg = float(json_data[0]['AVG(GWGoals)'])
    avg_otg = float(json_data[0]['AVG(OTGoals)'])
    avg_shg = float(json_data[0]['AVG(SHGoals)'])
    avg_shp = float(json_data[0]['AVG(SHPoints)'])
    avg_points = float(json_data[0]['AVG(Points)'])
    avg_pm = float(json_data[0]['AVG(PlusMinus)'])

    std_assists = float(json_data[0]['stddev(Assists)'])
    std_goals = float(json_data[0]['stddev(Goals)'])
    std_penm = float(json_data[0]['stddev(PenMinutes)'])
    std_shots = float(json_data[0]['stddev(Shots)']) 
    std_gp = float(json_data[0]['stddev(GamesPlayed)'])
    std_ppg = float(json_data[0]['stddev(PPGoals)'])
    std_ppp = float(json_data[0]['stddev(PPPoints)'])
    std_spct = float(json_data[0]['stddev(ShotPct)'])
    std_gwg = float(json_data[0]['stddev(GWGoals)'])
    std_otg = float(json_data[0]['stddev(OTGoals)'])
    std_shg = float(json_data[0]['stddev(SHGoals)'])
    std_shp = float(json_data[0]['stddev(SHPoints)'])
    std_points = float(json_data[0]['stddev(Points)'])
    std_pm = float(json_data[0]['stddev(PlusMinus)'])

    assists = float(player_data[0]['Assists'])
    goals = float(player_data[0]['Goals'])
    penm = float(player_data[0]['PenMinutes'])
    shots = float(player_data[0]['Shots']) 
    gp = float(player_data[0]['GamesPlayed'])
    ppg = float(player_data[0]['PPGoals'])
    ppp = float(player_data[0]['PPPoints'])
    spct = float(player_data[0]['ShotPct'])
    gwg = float(player_data[0]['GWGoals'])
    otg = float(player_data[0]['OTGoals'])
    shg = float(player_data[0]['SHGoals'])
    shp = float(player_data[0]['SHPoints'])
    points = float(player_data[0]['Points'])
    pm = float(player_data[0]['PlusMinus'])

    z_scores = {}
    z_scores["Assists"] = (assists - avg_assists) / std_assists
    z_scores["Goals"] = (goals - avg_goals) / std_goals
    z_scores["PIMs"] = ((penm - avg_penm) / std_penm) * -1
    z_scores["Shots"] = (shots - avg_shots) / std_shots
    z_scores["Games"] = (gp - avg_gp) / std_gp
    z_scores["PPGoals"] = (ppg - avg_ppg) / std_ppg
    z_scores["PPPoints"] = (ppp - avg_ppp) / std_ppp
    z_scores["Spct"] = (spct - avg_spct) / std_spct
    z_scores["GWG"] = (gwg - avg_gwg) / std_gwg
    z_scores["OTG"] = (otg - avg_otg) / std_otg
    z_scores["SHG"] = (shg - avg_shg) / std_shg
    z_scores["SHP"] = (shp - avg_shp) / std_shp
    z_scores["Points"] = (points - avg_points) / std_points
    z_scores["PM"] = (pm - avg_pm) / std_pm
    return z_scores

@app.route("/player")
@cross_origin()
def player():
    args = request.args
    playerid = args.get("playerid")
    season = args.get("season", default="20222023")
    if playerid == None: return jsonify({"message":"Please provide the unique player id as the parameter 'playerid'."})
    try:
        averages = sql.GetAveragesAndStDevForSeason(season)
        player_data = sql.GetPlayerTotalsForSeason(playerid, season)
        z_scores = calculate_z(averages, player_data)
        pre_shots = sql.GetPlayerShotsForSeason(playerid, season)
        shots = format_shots(pre_shots)
        return jsonify({"message": {"data": z_scores, "shots": shots}})
    except Exception as err:
        print(err)
        return jsonify({"message": "There was an error: {}".format(type(err))})

@app.route("/goalie")
@cross_origin()
def goalie():
    args = request.args
    playerid = args.get("playerid")
    season = args.get("season", default="20222023")
    if playerid == None: return jsonify({"message":"Please provide the unique goalie id as the parameter 'playerid'."})
    try:
        xG = sql.GetXGFacedByGoalie(playerid, season)
        ga = sql.GetGoalsAllowedByGoalie(playerid, season)
        pre_shots = sql.GetCoordinatesOfShotsForGoalie(playerid, season)
        shots = format_shots(pre_shots)
        return jsonify({"message": {"xG":xG, "GA":ga, "shots":shots, "season": season}})
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})
    
@app.route("/skaters")
@cross_origin()
def skaters():
    args = request.args
    season = args.get("season", default="20222023")
    try:
        json_data = sql.GetSkatersTableData(season)
        message = {"data": json_data, "season": season}
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

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
    if team == None: return jsonify({"message":"Please provide the team id in the format 'MIN' as the parameter team."})
    try:
        teamId = teamcodes[team.upper()]
        json_data = []
        if taken == "taken": 
            if start != None and end != None:
                pre_shots = sql.GetShotsForTeamInRange(teamId, season, start, end)
                shots = format_shots(pre_shots)
                json_data = {"shots": shots}
                message = {"data": json_data}
            else:
                pre_shots = sql.GetShotsForTeam(teamId, season)
                shots = format_shots(pre_shots)
                json_data = {"shots": shots}
                message = {"data": json_data}
        elif taken == "conceded":
            if start != None and end != None:
                pre_shots = sql.GetShotsAgainstTeamInRange(teamId, season, start, end)
                shots = format_shots(pre_shots)
                json_data = {"shots": shots}
                message = {"data": json_data}
            else:
                pre_shots = sql.GetShotsAgainstTeam(teamId, season)
                shots = format_shots(pre_shots)
                json_data = {"shots": shots}
                message = {"data": json_data}
        return jsonify({"message": message})
    except KeyError:
        return jsonify({"message": "There is no record of that team. Please provide the team id in the format 'MIN' as the parameter team."})
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

@app.route("/skaters-percent")
@cross_origin()
def skatersPercent():
    args = request.args
    season = args.get("season", default="20222023")
    player = args.get("id")
    if player == None: return jsonify({"message":"Please provide the unique player id as the parameter id."})
    try:
        json_data = sql.GetSkatersPercent(player, season)
        return jsonify({"message": {"data": json_data}})
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})
    
@app.route("/teams-stats")
@cross_origin()
def teamStats():
    args = request.args
    season = args.get("season", default="20222023")
    statType = args.get("type", default="0")
    statType = int(statType)
    try:
        if statType == 0: json_data = sql.GetXGFandXGAForAllTeamsSeason(season)
        if statType == 1: json_data = sql.GetGFandGAForAllTeamsSeason(season)
        if statType == 2: json_data = sql.GetSFandSAForAllTeamsSeason(season)
        if statType == 3: json_data = sql.GetFFandFAForAllTeamsSeason(season)
        return jsonify({"message": {"data": json_data}})
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

@app.route("/teams")
@cross_origin()
def teams():
    try:
        json_data = sql.GetAllTeams()
        message = {"data": json_data}
        return jsonify({"message": message})
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

@app.route("/live-game")
@cross_origin()
def livegame():      
    args = request.args
    id = args.get("id")
    if not id: return jsonify({"message":"Please provide the game id."})
    try:
        return jsonify({"message": processor.ProcessGameForAPI(id)})
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

@app.route("/rosters")
@cross_origin()
def rosters():      
    args = request.args
    year = args.get("year", default=20232024)
    id = args.get("id")
    if id is None: return jsonify({"message":"Please list the team name in the format 'MIN' as parameter 'id'."})
    try:
        return nhl.GetRoster(id, year)
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

@app.route("/player-proxy")
@cross_origin()
def playerproxy():      
    args = request.args
    id = args.get("id")
    if id is None: return jsonify({"message":"Please list the player id."})
    try:
        return nhl.GetPlayerLanding(id)
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})

@app.route("/score-proxy")
@cross_origin()
def scoreproxy():      
    gameData = nhl.GetLiveGames()
    today = gameData['currentDate']
    try:
        data = sql.GetGamesAndWinProbability(today)
        if len(data) > 0:
            gameDict = dict((x, y) for x, y in data)
            if len(gameDict) == len(gameData['games']):
                print(gameDict)
                for game in gameData['games']:
                    if game['id'] in gameDict.keys():
                        if gameDict[game['id']] is None: break
                        homeWinProbability = float(gameDict[game['id']]) * 100
                        game['homeTeam']['winProbability'] = round(homeWinProbability, 2)
                        game['awayTeam']['winProbability'] = round((100 - homeWinProbability), 2)
                        
                        if homeWinProbability >= 50:
                            game['homeTeam']['prediction'] = 'Favored'
                            game['awayTeam']['prediction'] = 'notFavored'
                        else:
                            game['homeTeam']['prediction'] = 'notFavored'
                            game['awayTeam']['prediction'] = 'Favored'
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})
    return gameData if gameData else jsonify({"message": "There was no data."})

def format_shot(x, y, eventTeam, playType, xG, period, homeId):
    if eventTeam == homeId: result = 'rgb(255, 99, 132)'
    else: result = 'rgb(54, 162, 235)'
    xG = (xG * 100) + 5
    if playType == 'goal': result = 'Black'

    if period % 2 == 0:
        x = x * -1
        y = y * -1

    x = (x * 6) + 600
    y = ((y * -1) * 6) + 255

    return {"x":x, "y": y, "xG":xG, "Result": result }

@app.route("/game-shots")
@cross_origin()
def get_single_game_shots():
    args = request.args
    gameId = args.get("gameId")
    homeshotsByTime, awayshotsByTime, homeXgByTime, awayXgByTime, times, shots = [],[],[],[],[],[]
    data = sql.GetAllEventsForAGame(gameId)
    for row in data:
        period = int(row[0])
        time = float(row[1].replace(":", "."))
        if period == 2: time = time + 20.0
        if period == 3: time = time + 40.0
        if period == 4: time = time + 60.0 
        times.append(round(time, 2))
        homeshotsByTime.append(int(row[2]) if row[2] else 0)
        awayshotsByTime.append(int(row[3]) if row[3] else 0)
        homeXgByTime.append(round(float(row[4]), 2) if row[4] else 0)
        awayXgByTime.append(round(float(row[5]), 2) if row[5] else 0)
        shots.append(format_shot(row[6], row[7], row[8], row[9], row[10], period, row[11]))
    shotsByTime = {"times": times, "homeShots": homeshotsByTime, "awayShots": awayshotsByTime, "homexG": homeXgByTime, "awayxG": awayXgByTime, "shots":shots}
    return shotsByTime

@app.route("/game-goals")
@cross_origin()
def get_goals():
    args = request.args
    gameId = args.get("gameId")
    data = sql.GetGoalsForGame(gameId)
    goals = []
    homeScore, awayScore = 0,0
    for goal in data:
        if goal[5] == goal[6]: homeScore += 1
        else: awayScore += 1
        goals.append({"Period": goal[0], "PeriodTime":goal[1], "Player1":goal[2], "PlayerName":goal[3], "TeamName":goal[4], "HomeScore":homeScore, "AwayScore":awayScore})
    return goals

@app.route("/games")
@cross_origin()
def games_by_team_date():
    args = request.args
    id = args.get("id")
    date = args.get("date")
    try:
        json_data = sql.GetGamesByTeamOrDate(id, date)
        return jsonify(json_data)
    except Exception as err:
        return jsonify({"message": "There was an error: {}".format(type(err))})
    
if __name__ == '__main__': app.run()