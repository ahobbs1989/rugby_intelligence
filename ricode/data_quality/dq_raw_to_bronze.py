import glob
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from ricode.utils import global_parameters as gp



class RTBDataQualityProcessor:
    def __init__(self):
        self._setup_environment()
        self.spark = self._build_spark_session()
        self.dq_rtb_game_stat_schema = StructType([
            StructField('raw_file', StringType()),
            StructField('bronze_file', StringType()),
            StructField('file_status', StringType())
        ])

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

    def process_game_stat_files(self):
        """Process raw and bronze game stat files and write the results to Delta Lake."""
        game_stat_files = self._get_game_stat_files()
        raw_game_stats = self._extract_raw_game_stats(game_stat_files)
        df_raw_game_stats = self.spark.createDataFrame([(x,) for x in raw_game_stats], ['raw_file'])

        bronze_game_stats = self._get_bronze_game_stats()
        df_bronze_game_stats = self.spark.createDataFrame([(x,) for x in bronze_game_stats], ['bronze_file'])

        df_rtb_game_stat_diff_result = self._compare_and_label_files(df_raw_game_stats, df_bronze_game_stats)
        self._write_to_delta(df_rtb_game_stat_diff_result)

    def _get_game_stat_files(self):
        """Retrieve all game stat files from the raw directory."""
        game_stat_files = []
        for root, _, files in os.walk(gp.RAW_GAME_STAT_PATH):
            game_stat_files.extend([(root, file) for file in files if 'game_stat' in file])
        return game_stat_files

    def _extract_raw_game_stats(self, game_stat_files):
        """Extract raw game stats from file names."""
        return [
            game_stat_file.replace('rpi_', '').replace('-vs-', '_').replace('_game_stats.txt', '')
            for _, game_stat_file in game_stat_files
        ]

    def _get_bronze_game_stats(self):
        """Retrieve all bronze game stats from the bronze directory."""
        return [
            f.split('\\')[-2]
            for f in glob.glob(os.path.join(gp.BRONZE_GAME_STAT_PATH, '*/'))
            if os.path.isdir(f)
        ]

    def _compare_and_label_files(self, df_raw_game_stats, df_bronze_game_stats):
        """Compare raw and bronze files and label their statuses."""
        df_rtb_game_stat_diff = df_raw_game_stats.join(
            df_bronze_game_stats,
            df_raw_game_stats.raw_file == df_bronze_game_stats.bronze_file,
            "left"
        )
        return df_rtb_game_stat_diff.withColumn(
            'file_status',
            when(col('raw_file').isNotNull() & col('bronze_file').isNotNull(), 'bronze_loaded')
            .when(col('raw_file').isNotNull() & col('bronze_file').isNull(), 'bronze_missing')
        )

    def _write_to_delta(self, df):
        """Write the resulting DataFrame to Delta Lake."""
        df.write.format('delta').mode('overwrite').save(os.path.join(gp.DATA_QUALITY_PATH, 'rtb_game_stat'))


if __name__ == "__main__":
    processor = RTBDataQualityProcessor()
    processor.process_game_stat_files()

