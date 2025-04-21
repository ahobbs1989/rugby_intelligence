import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils import file_utils as fu
from ricode.utils import global_parameters as gp
import requests
from bs4 import BeautifulSoup as bs
import re


def game_team_scrape(game_url: str | None, game_date: str, game_year: str) -> None:
    # Extract data from game_url
    home_team_re_match = re.search(r'live/([^/]+)-vs-', game_url)
    away_team_re_match = re.search(r'-vs-([^/]+)/', game_url)
    game_teams_re_match = re.search(r'live/([^/]+)', game_url)
    if home_team_re_match:
        home_team = home_team_re_match.group(1)
    if away_team_re_match:
        away_team = away_team_re_match.group(1)
    if game_teams_re_match:
        game_teams = game_teams_re_match.group(1)
    file_name = f'{game_teams}_{game_date}'

    print(f'##~~~~~~~~## Collecting teams  for : {file_name}')
    # Extract game level stats
    soup = bs(requests.get(game_url).content, 'html.parser')
    game_teams = soup.find_all(
        'section', class_='team-field component-box team-events')

    fu.write_raw_file(
        root_folder=gp.RAW_PATH,
        team=home_team,
        year=game_year,
        stat='teams',
        file_prefix=f'rpi_{file_name}',
        raw=game_teams
    )
