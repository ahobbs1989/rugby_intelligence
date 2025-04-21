import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from datetime import datetime
import glob
from bs4 import BeautifulSoup as bs
from web_to_raw import international_game_team_scrape as gts
from web_to_raw import international_game_stats_scrape as gs
from ricode.utils import file_utils as fu
from ricode.utils import global_parameters as gp


for team in gp.TEAM_LIST:
    raw_game_folder = f'{gp.RAW_PATH}\\{team}\\*.txt'

    # Extract game_url, game_date and game_year
    files = glob.glob(raw_game_folder)
    # Loop through all game year files
    for file in files:
        open_file = open(file, 'r')
        file_contents = open_file.read()
        soup = bs(file_contents, 'html.parser')
        games_html = soup.find_all('div', class_='games-list-item')
        # Loop through all games in game year files
        for game_html in games_html:
            game_date_html = game_html.find(
                'div', class_='date').get_text(strip=True)
            game_date, game_year = fu.format_and_return_date(game_date_html)
            game_url_html = game_html.find('a', class_='link-box')
            game_url = game_url_html['href'] if game_url_html else None
            print(
                f'~~~~~~~~Scraping data for: {team} - {game_date} - {game_url}~~~~~~~~')
            # game_url = str(game_url).replace('/?g', '/stats/?g')
            game_url = str(game_url).replace('/?g', '/teams/?g')
            if game_url == 'None':
                print(
                    f'!!~~~~~~~~No game_stats URL available for: {team} - {game_date}~~~~~~~~!!')
            else:
                gs.game_stats_scrape(
                    game_url=game_url, game_date=game_date, game_year=game_year)
                gts.game_team_scrape(
                    game_url=game_url, game_date=game_date, game_year=game_year)
