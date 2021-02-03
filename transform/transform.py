from pyspark.sql.dataframe import *
from sparkConf.logging import Log4j


def transform_boston_data(df: DataFrame, log: Log4j):
    """
    Function used to
    :param log:
    :param df:
    """
    df = remove_null(df,log)
    df = df.filter('indus > 7')
    log.warn("Data got Filtered and transformed")
    return df


def remove_null(df: DataFrame, log: Log4j):
    """

    :param log:
    :param df:
    :return:
    """
    non_null_df = df.dropna()
    log.warn("Null Values Removed")
    return non_null_df
