import os
import sys
import glob
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import to_date, to_timestamp, concat_ws, col, md5, concat_ws, least, greatest
from pyspark.sql import SparkSession
from ricode.data_quality import dq_bronze_to_silver as dq
from ricode.utils import global_parameters as gp
from ricode.utils import file_utils as fs

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

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


silver_df_union = None
for game in glob.glob(os.path.join(gp.BRONZE_GAME_PATH, '*/*/')):
    print(f'~~~~~~~~Loading file {game} from bronze to silver~~~~~~~~')
    df = spark.read.format('delta').load(game)

    if silver_df_union is None:
        silver_df_union = df
    else:
        silver_df_union = silver_df_union.unionByName(
            df, allowMissingColumns=True)

silver_df_union = silver_df_union \
    .withColumn("datetime",to_timestamp(concat_ws(" ", col("date"), col("time")),"EEE d MMM, yyyy h:mma")) \
    .withColumn("date", to_date(col("datetime"))) \
    .withColumn("home_score", col("home_score").cast("int")) \
    .withColumn("away_score", col("away_score").cast("int")) \
    .withColumn("point_difference", greatest("home_score", "away_score") - least("home_score", "away_score")) \
    .withColumn("team1", least("home_team", "away_team")) \
    .withColumn("team2", greatest("home_team", "away_team")) \
    .withColumn("game_key", md5(concat_ws("||", "team1", "team2", "date"))) \


silver_df_clean = silver_df_union.drop("time", "team1", "team2")
silver_df_ordered = silver_df_clean.orderBy('datetime', ascending=True).dropDuplicates()

silver_df_ordered.show()

silver_df_ordered.write \
  .format("delta") \
  .mode("overwrite") \
  .save(gp.SILVER_GAME_PATH) 


# Run DataQuality - bronze to silver
BTS_DataQuality = dq.BTSDataQualityProcessor()
BTS_DataQuality.run_all_checks()

data_quality_df = spark.read.format('delta').load(os.path.join(gp.DATA_QUALITY_PATH, 'bts_game'))
data_quality_df.show()
