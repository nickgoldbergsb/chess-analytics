from chessdotcom import get_player_games_by_month, get_player_stats
from datetime import datetime
import json

currentMonth = datetime.now().month
currentYear = datetime.now().year
currentDay = datetime.now().day
date_string = f"{currentYear}-{currentMonth}-{currentDay}"

def get_monthly_games(username, year=currentYear, month=currentMonth):
    response = get_player_games_by_month(
        username=username, 
        year=year,
        month=month).json
    raw_monthly_games = response['games']

    with open('resources/data/raw_monthly_games.json', 'w') as f:
        json.dump(raw_monthly_games, f)

def get_rapid_game_statistics(username):
    response = get_player_stats(
        username=username).json
    raw_chess_rapid_statistics = response['stats']['chess_rapid']

    with open(f'resources/data/raw_chess_rapid_statistics.json', 'w') as f:
        json.dump(raw_chess_rapid_statistics, f)

def get_blitz_game_statistics(username):
    response = get_player_stats(
        username=username).json
    raw_chess_blitz_statistics = response['stats']['chess_blitz']

    with open(f'resources/data/raw_chess_blitz_statistics.json', 'w') as f:
        json.dump(raw_chess_blitz_statistics, f)