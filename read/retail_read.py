from schemas.boston_schema import boston_schema
from pyspark.sql import SparkSession


def read_order_data(spark: SparkSession, config):
    """
    Function used to read the data from CSV
    :param config:
    :param spark:
    """
    data = spark.read.schema(boston_schema).option("header", "true").csv(config["olist_order_dataset_csv_path"])
    print("CSV Files Read")
    return data



