from bs4 import BeautifulSoup as bs
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import time
import re
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils import global_parameters as gp
from ricode.utils import file_utils as fu


def game_stats_scrape(game_url : str|None, game_date: str, game_year: str) -> None:
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
    
    
    run_files = fu.stats_collected ()
    if file_name in run_files:
        print(f'##~~~~~~~~## Stats already collected for: {file_name} ##~~~~~~~~## ')

    else:
        print(f'##~~~~~~~~## Collecting stats  for : {file_name}')
        # Extract game level stats
        soup =  bs(requests.get(game_url).content, 'html.parser')
        game_stats = soup.find_all('section', class_='poss-ter component-box')
        
        fu.write_raw_file(
                        root_folder=f'C:\\Users\\ahobb\\OneDrive\\RugbyIntelligence\\raw',
                        team = home_team,
                        year = game_year,
                        stat = 'game_stats',
                        file_prefix = f'rpi_{file_name}',
                        raw = game_stats
                    )
        # # Extract player level stats
        sleep_time = 1

        # Set up the Chrome driver options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        service = Service("C:\Developer\Python\chromedriver-win64\chromedriver.exe")
        options = Options()
        options.add_argument("--log-level=1")
        options.add_experimental_option("detach", True)
        driver = webdriver.Chrome(options=options)

            
        #Navigate to the game url page
        driver.get(game_url)
        time.sleep(sleep_time)
        driver.maximize_window()
        time.sleep(sleep_time)

        
        #Accept cookies and scroll to top
        cookie_accept = driver.find_element(
            By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'
        ).click()
        time.sleep(sleep_time)
        driver.execute_script("window.scrollTo(0,0)")
        
        
        #Expand player stats drop down
        try:
            driver.find_element(
                By.XPATH, f'/html/body/main/div[3]/div[1]/div[3]/section[1]/h2/span[2]/img'
            ).click()
            time.sleep(sleep_time)
            
            player_stats = ["carries","clean_breaks","completed_tackles","turnovers_conceded","turnovers_won","carries","total_kicks","passes",
            "carries_metres","clean_breaks","offloads","defenders_beaten","try_assists","tries","turnovers_conceded","carries_per_minute",
            "total_tackles","missed_tackles","completed_tackles","dominant_tackles","turnovers_won","ruck_turnovers","lineouts_won",
            "tackles_per_minute","red_cards","yellow_cards","penalties_conceded"]

            for player_stat in gp.PLAYER_STATS:
                #Open the drop down
                driver.find_element(By.XPATH, "/html/body/main/div[3]/div[1]/div[3]/div/section/div[1]/div[1]/span").click()
                time.sleep(sleep_time)
                
                #Load the stat
                driver.find_element(By.CSS_SELECTOR, f"[data-id={player_stat}]").click()
                
                time.sleep(sleep_time)
                
                #Scrape the stat data
                html = driver.page_source
                soup = bs(html, "html.parser")
                player_stats_raw = soup.find_all('div', id='players-selector-result')
                
                #Write to file
                fu.write_raw_file(
                            root_folder=f'C:\\Users\\ahobb\\OneDrive\\RugbyIntelligence\\raw',
                            team = home_team,
                            year = game_year,
                            stat = player_stat,
                            file_prefix = f'rpi_{file_name}',
                            raw = player_stats_raw
                        )
        except Exception as e:
            print(f'!!~~~~~~~~!! Unable to download stats for: {game_url} !!~~~~~~~~!!')
            # driver.close()
        driver.close()
