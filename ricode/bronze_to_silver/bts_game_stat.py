import os
import sys
import glob
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils import file_utils as fs
from ricode.utils import global_parameters as gp
from ricode.data_quality import dq_bronze_to_silver as dq
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import concat_ws, col, md5, concat_ws, lit, regexp_replace
from datetime import datetime
from delta import configure_spark_with_delta_pip

## The python environment variables need to be setup correct in Windows path ##
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



# Build spark session
builder = SparkSession.builder \
    .master("local[*]") \
    .appName("BronzeToSilverGame") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12-2.4.0")
spark = configure_spark_with_delta_pip(builder).getOrCreate()\


game_stat_schema = StructType([
    StructField('team', StringType()),
    StructField('carries', IntegerType()),
    StructField('conversions', IntegerType()),
    StructField('drop_goals', IntegerType()),
    StructField('kick_to_pass_ratio', StringType()),
    StructField('line_breaks', IntegerType()),
    StructField('lineouts', IntegerType()),
    StructField('lineout_win_percent', IntegerType()),
    StructField('passes', IntegerType()),
    StructField('penalty_goals', IntegerType()),
    StructField('possession_percent', IntegerType()),
    StructField('post_contact_metres', IntegerType()),
    StructField('red_cards', IntegerType()),
    StructField('restarts_received', IntegerType()),
    StructField('restarts_received_win_percent', IntegerType()),
    StructField('scrums', IntegerType()),
    StructField('scrums_win_percent', IntegerType()),
    StructField('tackle_completion_percent', IntegerType()),
    StructField('tackles_made', IntegerType()),
    StructField('tackles_missed', IntegerType()),
    StructField('territory_percent', IntegerType()),
    StructField('total_kicks', IntegerType()),
    StructField('tries', IntegerType()),
    StructField('turnovers_lost', IntegerType()),
    StructField('turnovers_won', IntegerType()),
    StructField('yellow_cards', IntegerType()),
    StructField('game_key', StringType())
])




# Loop thorough bronze files and build silver df
silver_df_union = None
for game_stat_file in glob.glob(os.path.join(gp.BRONZE_GAME_STAT_PATH, '*/')):
    game_stat_file_clean = game_stat_file.replace(
        gp.BRONZE_GAME_STAT_PATH, '').replace('\\', '')
    print(f'~~~~~~~~Loading file {game_stat_file_clean} from bronze to silver~~~~~~~~')

    # Get file name details for game key
    team1 = fs.camel_case(game_stat_file_clean.split('_')[0])
    team2 = fs.camel_case(game_stat_file_clean.split('_')[1])
    date_obj = datetime.strptime(
        game_stat_file_clean.split('_')[2], "%Y%m%d")
    date = date_obj.strftime("%Y-%m-%d")

    df = spark.read.format('delta').load(game_stat_file)
    home_team_df = df.selectExpr(
        "stat", "home_team_stat as value").withColumn("team", lit("Home"))
    away_team_df = df.selectExpr(
        "stat", "away_team_stat as value").withColumn("team", lit("Away"))

    stacked_df = home_team_df.union(away_team_df)

    pivoted_df = stacked_df.groupBy("team").pivot(
        "stat").agg({"value": "first"})
    enriched_df = pivoted_df.withColumn("team1", lit(team1))\
                            .withColumn("team2", lit(team2))\
                            .withColumn("date", lit(date))
    silver_stat_df = enriched_df.withColumn(
        "game_key", md5(concat_ws("||", "team1", "team2", "date")))

    if silver_df_union is None:
        silver_df_union = silver_stat_df
    else:
        silver_df_union = silver_df_union.unionByName(
            silver_stat_df, allowMissingColumns=True)


# Clean and correct df datatypes
cast_cols = [
    "Carries", "Conversions", "Drop Goals", "Line Breaks", "Lineout", "Passes",
    "Penalties Conceded", "Penalty Goals", "Red Cards", "Restarts Received",
    "Scrums", "Tackles Made", "Tackles Missed", "Total Kicks", "Tries",
    "Turnovers Lost", "Turnovers Won", "Yellow Cards"
]


percent_cols = [
    "Lineout Win %", "Possession", #"Possession Last 10 min",
    "Restarts Received Win %", "Scrum Win %", "Tackle Completion %",
    "Territory"
]


meter_cols = ["Post Contact Metres"]


df = silver_df_union


for col_name in cast_cols:
    df = df.withColumn(col_name, col(col_name).cast("int"))

for col_name in percent_cols:
    df = df.withColumn(col_name, regexp_replace(
        col(col_name), "%", "").cast("int"))

for col_name in meter_cols:
    df = df.withColumn(col_name, regexp_replace(
        col(col_name), "m", "").cast("int"))



silver_df_clean = df


rename_map = {
    "Carries": "carries",
    "Conversions": "conversions",
    "Drop Goals": "drop_goals",
    "Kick To Pass Ratio": "kick_to_pass_ratio",
    "Line Breaks": "line_breaks",
    "Lineout": "lineouts",
    "Lineout Win %": "lineout_win_percent",
    "Passes": "passes",
    "Penalty Goals": "penalty_goals",
    "Possession": "possession_percent",
    # "Possession Last 10 min": "possession_last_10_min",
    "Post Contact Metres": "post_contact_metres",
    "Red Cards": "red_cards",
    "Restarts Received": "restarts_received",
    "Restarts Received Win %": "restarts_received_win_percent",
    "Scrums": "scrums",
    "Scrum Win %": "scrums_win_percent",
    "Tackle Completion %": "tackle_completion_percent",
    "Tackles Made": "tackles_made",
    "Tackles Missed": "tackles_missed",
    "Territory": "territory_percent",
    "Total Kicks": "total_kicks",
    "Tries": "tries",
    "Turnovers Lost": "turnovers_lost",
    "Turnovers Won": "turnovers_won",
    "Yellow Cards": "yellow_cards",
    "Team": "team",
    "Game Key": "game_key"
}

# Rename columns and bind to schema
for original, renamed in rename_map.items():
    if original in silver_df_clean.columns:
        silver_df_clean = silver_df_clean.withColumnRenamed(original, renamed)


schema_columns = [field.name for field in game_stat_schema.fields]

silver_df_clean_aligned = silver_df_clean.select(*schema_columns)

silver_df_final = spark.createDataFrame(
    silver_df_clean_aligned.rdd, schema=game_stat_schema)

silver_df_final.show()

silver_df_final.write \
    .format("delta") \
    .mode("append") \
    .save(gp.SILVER_GAME_STAT_PATH) 


spark.stop()