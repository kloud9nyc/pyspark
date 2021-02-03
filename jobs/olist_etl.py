from sparkConf.session_olist import create_spark_session
from read.read_olist import *
from transform.transform_olist import *
from write.write import write_csv_local


def main() -> object:
    """Main ETL script definition.

    :return: None
    """

    # start Spark application and get Spark session, logger and config
    spark, config, log = create_spark_session('Olist App')
    log.warn('Job is up-and-running')

    # # Read the Data
    order_df = read_order_data(spark, config, log)
    order_items_df = read_order_items(spark, config, log)
    #
    # # Transform the data
    transformed_df = calculate_order_sales(order_items_df, log)

    transformed_df = calculate_order_sales_for_delivered(order_df, order_items_df, log)
    print(transformed_df.groupBy().sum('total_price').collect()[0][0])
    #
    # # Write Output
    # write_csv_local(transformed_df, config, log)
    #
    # log.warn(" Job Ended")
    spark.stop()
    return None


if __name__ == '__main__':
    main()
