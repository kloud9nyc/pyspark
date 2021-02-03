from os import environ, listdir

from pyspark import SparkFiles
from pyspark.sql import SparkSession
import boto3
import json

local_config_file_location = "../configs/retail_dev_config.json"
app_name="Olist_Retail_App"


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


def transform(row):
    """

    :param row:
    :return:
    """
    # Add a new key to each row
    row["new_key"] = "my_new_value"
    return row


def create_spark_session():
    """
    This function to create the spark session
    """
    env = 'ENV' in environ.keys()
    environment = environ.get('ENV')
    s3 = boto3.client('s3')

    if env:
        print("Configured Environment: " + environment)
        if environment == 'dev':
            print("jio")
            print("Local Mode Session")
            spark_builder = (
                SparkSession
                    .builder
                    .appName("sales_stg_prd_job")
                    .master("local")
            )

            spark = spark_builder.getOrCreate()

            with open("../configs/dev_config.json") as configfile:
                local_config = json.load(configfile)
                print(local_config)

            print("Spark Session Created")
            return spark, local_config
        elif environment == 'local':
            print("Local Mode Session")
            spark_builder = (
                SparkSession
                    .builder
                    .appName("sales_stg_prd_job")
                    .master("local")
                    .config("driver-memory", "1g")
                    .config("--num-executors", "2")
            )

            spark = spark_builder.getOrCreate()

            print(spark.sparkContext.getConf().getAll())
            d_params = spark.sparkContext.getConf().get('spark.driver.extraJavaOptions').split(' ')

            local_config_file = ''
            for configs in d_params:
                print(configs)
                config_x = configs.split('=')
                print(config_x)
                if config_x[0] == '-Dconfig_url':
                    print(config_x[1])
                    local_config_file = config_x[1]
            print(local_config_file)
            local_config = None
            with open(local_config_file) as configfile:
                local_config = json.load(configfile)
                print(local_config)

            print("Spark Session Created")
            return spark, local_config
        else:
            print("No Defined Environment found")

    else:
        print("Production/Cloud Mode Session")
        spark_builder = (
            SparkSession
                .builder
                .appName("sales_stg_prd_job")
        )

        spark = spark_builder.getOrCreate()

        d_params = spark.sparkContext.getConf().get('spark.driver.extraJavaOptions').split(' ')

        s3_config_key = ''
        for configs in d_params:
            print(configs)
            config_x = configs.split('=')
            print(config_x)
            if config_x[0] == '-Dconfig_url':
                print(config_x[1])
                s3_config_key = config_x[1]

        if s3_config_key == '':
            raise Exception("Running in Production Mode, No -Dconfig_url key found for S3 Config URL")
        bucket, key = split_s3_path(s3_config_key)

        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body']

        s3config = json.loads(content.read())

        print("Spark Session Created")
        return spark, s3config
