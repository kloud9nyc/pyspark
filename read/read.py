from schemas.boston_schema import boston_schema
from pyspark.sql import SparkSession

from sparkConf.logging import Log4j


def read_csv(spark: SparkSession, config, log: Log4j):
    """
    Function used to read the data from CSV
    :param log:
    :param config:
    :param spark:
    """
    data = spark.read.schema(boston_schema).option("header", "true").csv(config["boston_dataset_csv_path"])
    log.warn("CSV Files Read Completed")
    return data



