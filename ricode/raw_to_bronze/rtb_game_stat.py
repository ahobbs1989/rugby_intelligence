import os
import sys
from bs4 import BeautifulSoup
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils import global_parameters as gp
from ricode.data_quality import dq_raw_to_bronze as dq
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
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


game_stat_schema = StructType([
    StructField('stat', StringType()),
    StructField('home_team_stat', StringType()),
    StructField('away_team_stat', StringType())
])


# Check for missing bronze files
dq_rtb_game_stat = spark.read.format('delta').load(os.path.join(gp.DATA_QUALITY_PATH, 'rtb_game_stat')).filter(col('file_status') == 'bronze_missing').select('raw_file')
rtb_game_stat_diff = [row['raw_file'] for row in dq_rtb_game_stat.collect()]


game_stat_files = []
for root, dir, files in os.walk(gp.RAW_GAME_STAT_PATH):
    game_stat_files.extend([(root, file) for file in files if 'game_stat' in file])


for game_stat_file_path, game_stat_file in game_stat_files:
    file_name = game_stat_file.replace('rpi_', '').replace('-vs-', '_').replace('_game_stats.txt', '')
    file_read_path = os.path.join(game_stat_file_path, game_stat_file)
    file_write_name = os.path.join(gp.BRONZE_GAME_STAT_PATH, file_name)
    
    if file_name in rtb_game_stat_diff:
        print(f'~~~~~~~~Running raw to bronze extraction for {file_name}~~~~~~~~')
        with open (file_read_path, 'r', encoding='utf-8') as file:
            html = file.read()
        game_stat_soup = BeautifulSoup(html, 'html.parser')


        def extract_game_stat(soup: BeautifulSoup) -> any:
            for game_stat in game_stat_soup.find_all('div', class_='stat'):
                stat = game_stat.find('div', class_='mid smaller').get_text(strip=True)
                stat_numbers = [div.get_text(strip=True) for div in game_stat.find_all('div') if not div.has_attr('class')]
                
                yield(stat,stat_numbers[0],stat_numbers[1])

        
        game_stat_result = list(extract_game_stat(game_stat_soup))

        
        df = spark.createDataFrame(game_stat_result, game_stat_schema)
        df.write.format('delta').mode('overwrite').save(file_write_name)


    else:
        print(f'~~~~~~~~Bronze file already exists {file_name}~~~~~~~~')

# Run DataQuality - raw to bronze
RTB_DataQuality = dq.RTBDataQualityProcessor()
RTB_DataQuality.process_game_stat_files()




