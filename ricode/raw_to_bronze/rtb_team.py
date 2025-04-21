#################### Pyspark/Hadoop install Notes - This is a pain ##################
# Make sure to use same versions of Hadoop - can get from  https://sparkbyexamples.com/spark/spark-hadoop-exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io-nativeio-nativeiowindows-access0ljava-lang-stringiz/
# Check the winsutils runs correctly, may have dll error in which case: https://stackoverflow.com/questions/45947375/why-does-starting-a-streaming-query-lead-to-exitcodeexception-exitcode-1073741
# Check the system path variables are all correctly set

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from ricode.utils import global_parameters as gp



## The python environment variables need to be setup correct in Windows path ##
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Build the spark session
builder = SparkSession.builder \
    .master("local[*]") \
    .appName("PySpark Installation Test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12-2.4.0")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext


# Extract team names from raw folder names
teams = os.listdir(gp.RAW_GAME_PATH)
team_list = [[team]
             for team in teams if team not in ['rugby_pass', 'game_stats']]
schema = StructType([
    StructField("team", StringType(), True)
])
rdd = spark.sparkContext.parallelize(team_list)
df = spark.createDataFrame(rdd, schema)

# Write the dataframe as delta to bronze folder
df.write.format('delta').mode('overwrite').save(gp.BRONZE_TEAM_PATH)
