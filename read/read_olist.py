from schemas.olist_order_schema import olist_order_schema
from schemas.olist_order_item_schema import olist_order_item_schema
from pyspark.sql import SparkSession

from sparkConf.logging import Log4j


def read_order_data(spark: SparkSession, config, log: Log4j):
    """
    Function used to read the data from CSV
    :param log:
    :param config:
    :param spark:
    """
    data = spark.read.schema(olist_order_schema).option("header", "true").csv(config["olist_order_dataset_csv_path"])
    log.warn("Order Details received")

    return data


def read_order_items(spark: SparkSession, config, log: Log4j):
    """
    Function used to read the data from CSV
    :param log:
    :param config:
    :param spark:
    """
    data = spark.read.schema(olist_order_item_schema).option("header", "true").csv(config["olist_order_items_dataset_csv_path"])
    log.warn("Order Items received")

    return data
