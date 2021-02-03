"""
Olist_order_schema.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import pyspark.sql.types

olist_order_schema = pyspark.sql.types.StructType(
    [pyspark.sql.types.StructField("order_id", pyspark.sql.types.StringType(), False),
     pyspark.sql.types.StructField("customer_id", pyspark.sql.types.StringType(), False),
     pyspark.sql.types.StructField("order_status", pyspark.sql.types.StringType(), False),
     pyspark.sql.types.StructField("order_purchase_timestamp", pyspark.sql.types.DateType(), False),
     pyspark.sql.types.StructField("order_approved_at", pyspark.sql.types.TimestampType(), False),
     pyspark.sql.types.StructField("order_delivered_carrier_date", pyspark.sql.types.TimestampType(), False),
     pyspark.sql.types.StructField("order_delivered_customer_date", pyspark.sql.types.TimestampType(), False),
     pyspark.sql.types.StructField("order_estimated_delivery_date", pyspark.sql.types.TimestampType(), False)
     ])