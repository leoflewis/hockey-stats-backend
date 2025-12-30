from datetime import datetime, date


def FormatSeason(date: date) -> str:
    year = date.year
    month = date.month

    if month >= 10:  # October–December
        start_year = year
        end_year = year + 1
    else:  # January–September
        start_year = year - 1
        end_year = year

    return f"{start_year}{end_year}"

def GetTeamIdFromName(name: str) -> int:
    NHL_TEAMS = {
        "Anaheim Ducks": 24,
        "Arizona Coyotes": 53,
        "Boston Bruins": 6,
        "Buffalo Sabres": 7,
        "Calgary Flames": 20,
        "Carolina Hurricanes": 12,
        "Chicago Blackhawks": 16,
        "Colorado Avalanche": 21,
        "Columbus Blue Jackets": 29,
        "Dallas Stars": 25,
        "Detroit Red Wings": 17,
        "Edmonton Oilers": 22,
        "Florida Panthers": 13,
        "Los Angeles Kings": 26,
        "Minnesota Wild": 30,
        "Montréal Canadiens": 8,
        "Nashville Predators": 18,
        "New Jersey Devils": 1,
        "New York Islanders": 2,
        "New York Rangers": 3,
        "Ottawa Senators": 9,
        "Philadelphia Flyers": 4,
        "Pittsburgh Penguins": 5,
        "San Jose Sharks": 28,
        "Seattle Kraken": 55,
        "St. Louis Blues": 19,
        "Tampa Bay Lightning": 14,
        "Toronto Maple Leafs": 10,
        "Utah Mammoth": 59,
        "Vancouver Canucks": 23,
        "Vegas Golden Knights": 54,
        "Washington Capitals": 15,
        "Winnipeg Jets": 52
    }

    if name not in NHL_TEAMS: raise Exception('{name} not found.')
    
    return NHL_TEAMS[name]


