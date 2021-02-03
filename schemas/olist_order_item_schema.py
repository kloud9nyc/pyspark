"""
Olist_order_item_schema.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import pyspark.sql.types

olist_order_item_schema = pyspark.sql.types.StructType(
    [pyspark.sql.types.StructField("order_id", pyspark.sql.types.StringType(), False),
     pyspark.sql.types.StructField("order_item_id", pyspark.sql.types.IntegerType(), False),
     pyspark.sql.types.StructField("product_id", pyspark.sql.types.StringType(), False),
     pyspark.sql.types.StructField("seller_id", pyspark.sql.types.StringType(), False),
     pyspark.sql.types.StructField("shipping_limit_date", pyspark.sql.types.TimestampType(), False),
     pyspark.sql.types.StructField("price", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("freight_value", pyspark.sql.types.FloatType(), False)
     ])
