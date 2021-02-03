from os import environ, listdir

from pyspark import SparkFiles
from pyspark.sql import SparkSession
import boto3
import json

from sparkConf import logging

local_config_file_location = "../configs/dev_config.json"

config_url_key = "config_url"
spark_config_local_driver_memory_key = 'spark.driver.memory'
spark_config_local_driver_memory_value = '1g'
spark_config_local_executor_memory_key = 'spark.executor.memory'
spark_config_local_executor_memory_value = '1g'
local_master_url = 'local'


def split_s3_path(s3_path):
    """

    :param s3_path:
    :return:
    """
    print(s3_path)
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    print(bucket)
    key = "/".join(path_parts)
    print(key)
    return bucket, key


def create_spark_session(app_name):
    """
    This function to create the spark session
    """
    env = 'ENV' in environ.keys()
    environment = environ.get('ENV')
    s3 = boto3.client('s3')
    spark_logger = None

    if app_name == '':
        app_name = "Spark_default_app"
    if env:
        print("Configured Environment: " + environment)
        if environment == 'dev':
            print("Local Mode Session")
            spark_builder = (
                SparkSession
                    .builder
                    .appName(app_name)
                    .master(local_master_url)
            )

            spark = spark_builder.getOrCreate()
            spark_logger = logging.Log4j(spark)

            with open(local_config_file_location) as configfile:
                local_config = json.load(configfile)
                print(local_config)

            spark_logger.warn("Spark Session Created")
            return spark, local_config, spark_logger
        elif environment == 'local':
            print("Local Mode Session")
            spark_builder = (
                SparkSession
                    .builder
                    .appName(app_name)
                    .master(local_master_url)
                    .config(spark_config_local_driver_memory_key, spark_config_local_driver_memory_value)
                    .config(spark_config_local_executor_memory_key, spark_config_local_executor_memory_value)
            )

            spark = spark_builder.getOrCreate()
            spark_logger = logging.Log4j(spark)

            print(spark.sparkContext.getConf().getAll())
            d_params = spark.sparkContext.getConf().get('spark.driver.extraJavaOptions').split(' ')

            local_config_file = ''
            for configs in d_params:
                print(configs)
                config_x = configs.split('=')
                print(config_x)
                if config_x[0] == config_url_key:
                    print(config_x[1])
                    local_config_file = config_x[1]
            print(local_config_file)
            local_config = None
            with open(local_config_file) as configfile:
                local_config = json.load(configfile)
                print(local_config)

            spark_logger.warn("Spark Session Created")
            return spark, local_config, spark_logger
        else:
            print("No Defined Environment found")

    else:
        print("Production/Cloud Mode Session")
        spark_builder = (
            SparkSession
                .builder
                .appName(app_name)
        )

        spark = spark_builder.getOrCreate()
        spark_logger = logging.Log4j(spark)

        d_params = spark.sparkContext.getConf().get('spark.driver.extraJavaOptions').split(' ')

        s3_config_key = ''
        for configs in d_params:
            print(configs)
            config_x = configs.split('=')
            print(config_x)
            if config_x[0] == config_url_key:
                print(config_x[1])
                s3_config_key = config_x[1]

        if s3_config_key == '':
            raise Exception("Running in Production Mode, No -Dconfig_url key found for S3 Config URL")
        bucket, key = split_s3_path(s3_config_key)

        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body']

        s3config = json.loads(content.read())

        spark_logger.warn("Spark Session Created")
        return spark, s3config, spark_logger
