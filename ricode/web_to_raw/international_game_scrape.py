import time
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils import file_utils as fu
from ricode.utils import global_parameters as gp


for team in gp.TEAM_LIST:
    # Set up the Chrome driver options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    service = Service(f"{gp.CHROME_DRIVER_PATH}chromedriver.exe")
    options = Options()
    options.add_argument("--log-level=1")
    options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options)

    # Navigate to the team fixtures page
    driver.get(f"{gp.TARGET_SITE}teams/{team}/fixtures-results/")
    time.sleep(gp.SELENIUM_SLEEP)

    # Accept cookies and scroll to top
    cookie_accept = driver.find_element(
        By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'
    ).click()
    time.sleep(gp.SELENIUM_SLEEP)
    driver.execute_script("window.scrollTo(0,0)")

    # Loop through all years and write to file
    list_item = 0
    while list_item <= 13:
        dropdown_click = driver.find_element(
            By.XPATH,
            "/html/body/main/div/div/div[1]/div/div"
        )
        time.sleep(gp.SELENIUM_SLEEP)
        driver.execute_script("arguments[0].click();", dropdown_click)
        time.sleep(gp.SELENIUM_SLEEP)
        try:
            driver.find_element(
                By.XPATH, f'/html/body/main/div/div/div[1]/div/ul/li[{list_item}]'
            ).click()
            time.sleep(gp.SELENIUM_SLEEP)

            html = driver.page_source
            soup = bs(html, "html.parser")

            dates = soup.find_all("div", class_="date")
            year = fu.find_year_in_list(dates)

            games = soup.find_all("div", class_="fixtures-results")

            # Create raw team folder and write files per year
            fu.write_raw_file(
                root_folder=gp.RAW_PATH,
                team=team,
                year=year,
                file_prefix='rugby_pass_internationals',
                raw=games
            )
        except Exception as e:
            list_item += 1
            print(f'Error encountered: {e}')
            continue
        list_item += 1
