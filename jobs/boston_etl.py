from sparkConf.session import create_spark_session
from read.read import read_csv
from transform.transform import transform_boston_data
from write.write import write_csv_local


def main() -> object:
    """Main ETL script definition.

    :return: None
    """

    # start Spark application and get Spark session, logger and config
    spark, config, log = create_spark_session('Boston App')
    log.warn('Job is up-and-running')

    # Read the Data
    df = read_csv(spark, config, log)

    # Transform the data
    transformed_df = transform_boston_data(df, log)

    # Write Output
    write_csv_local(transformed_df, config, log)

    log.warn(" Job Ended")
    spark.stop()
    return None


if __name__ == '__main__':
    main()
