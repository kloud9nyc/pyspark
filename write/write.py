from pyspark.sql.dataframe import *

from sparkConf.logging import Log4j


def write_csv_local(df: DataFrame, config, log: Log4j):
    """
    Function to write the ouput to CSV Local Location
    :param log:
    :param df:
    """
    df.write.mode('overwrite').csv(config["boston_dataset_output_path"], header=True)
    log.warn("Output has written")
