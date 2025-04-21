import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils import global_parameters as gp
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip


## The python environment variables need to be setup correct in Windows path ##
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Build spark session
builder = SparkSession.builder \
    .master("local[*]") \
    .appName("RawToBronze_Game") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12-2.4.0")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


team_df = spark.read.format('delta').load(gp.BRONZE_TEAM_PATH)
team_list = [row['team'] for row in team_df.collect()]

for team in team_list:
    team_path = os.path.join(gp.RAW_GAME_PATH, team)
    print(f'~~~~~~~~Running extraction for {team}~~~~~~~~')

    for root, dir, files in os.walk(team_path):
        if files:
            for file in files:
                print(f'~~~~~~~~Running raw to bronze extraction for {file}~~~~~~~~')
                file_path = os.path.join(root, file)
                file_year = file.split('_')[-1].replace('.txt', '')
                file_write_name = os.path.join(
                    gp.BRONZE_PATH, 'game', file_year, f'{team}')

                with open(file_path, 'r', encoding='utf-8') as file:
                    html = file.read()

                game_soup = BeautifulSoup(html, 'html.parser')
               
                # Define schema
                game_schema = StructType([
                    StructField('date', StringType(), True),
                    StructField('time', StringType(), True),
                    StructField('venue', StringType(), True),
                    StructField('home_team', StringType(), True),
                    StructField('away_team', StringType(), True),
                    StructField('home_score', StringType(), True),
                    StructField('away_score', StringType(), True),
                    StructField('result', StringType(), True)
                ])

                def game_result(home_team: str, home_score: int, away_team: str, away_score: int) -> str:
                    return home_team if home_score > away_score else away_team if away_score > home_score else 'draw'

                def extract_game_data(soup: BeautifulSoup) -> any:
                    for game in soup.find_all('div', class_='games-list-item'):
                        date = game.find('div', class_='date').text.strip()
                        time = game.find(
                            'div', class_='game-time').text.strip()
                        venue = game.find('div', class_='venue').text.strip()
                        try:
                            home_team = game.find(
                                'div', class_='team home').text.strip()
                        except:
                            home_team = game.find(
                                'a', class_='team home').text.strip()
                        try:
                            away_team = game.find(
                                'div', class_='team away').text.strip()
                        except:
                            away_team = game.find(
                                'a', class_='team away').text.strip()
                        home_score = int(
                            game.find('div', class_='score home').text.strip())
                        away_score = int(
                            game.find('div', class_='score away').text.strip())
                        result = game_result(
                            home_team, home_score, away_team, away_score)

                        yield (date, time, venue, home_team, away_team, home_score, away_score, result)

                game_result = list(extract_game_data(game_soup))
                print(game_result)
                df = spark.createDataFrame(game_result, game_schema)

                df.write.format('delta').mode('overwrite').save(file_write_name)
