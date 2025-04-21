import os
import sys
import glob
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from ricode.utils import global_parameters as gp



class BTSDataQualityProcessor:
    def __init__(self):
        self._setup_environment()
        self.spark = self._build_spark_session()
        self.dq_bts_game_schema = StructType([
            StructField('data_quality_check', StringType()),
            StructField('value', StringType())
        ])
        self.df = self.spark.read.format('delta').load(gp.SILVER_GAME_PATH)

    def _setup_environment(self):
        """Set up the Python environment variables for PySpark."""
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    def _build_spark_session(self):
        """Build and return a Spark session configured for Delta Lake."""
        builder = SparkSession.builder \
            .master("local[*]") \
            .appName("DataQuality") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12-2.4.0")
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def _write_to_delta(self, df):
        """Write the resulting DataFrame to Delta Lake."""
        df.write.format('delta').mode('overwrite').save(os.path.join(gp.DATA_QUALITY_PATH, 'bts_game'))

    def check_empty_venue(self):
        return self.df.where("venue = ''")

    def check_same_teams(self):
        return self.df.where("home_team = away_team")

    def check_invalid_draws(self):
        return self.df.where("home_score = away_score and result <> 'draw'")

    def check_duplicates(self):
        return self.df.groupBy(*[col for col in self.df.columns]).count().filter(col("count") > 1)

    def check_missing_values(self):
        return self.df.select(
            *[
                ((col(c).isNull()) | (col(c) == "")).cast("int").alias(c)
                for c in self.df.columns
            ]
        ).groupBy().sum()

    def run_all_checks(self):
        checks = [
            ("empty_venue", self.check_empty_venue()),
            ("same_teams", self.check_same_teams()),
            ("invalid_draws", self.check_invalid_draws()),
            ("duplicates", self.check_duplicates()),
            ("missing_values", self.check_missing_values()),
        ]

        results = []
        for check_name, check_df in checks:
            count = check_df.count()
            results.append((check_name, count))

        df_bts_game = self.spark.createDataFrame(results, self.dq_bts_game_schema)
        self._write_to_delta(df_bts_game)
    
    

if __name__ == "__main__":
    processor = BTSDataQualityProcessor()
    processor.run_all_checks()




