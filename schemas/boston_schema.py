"""
boston_schema.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import __main__
import pyspark.sql.types

boston_schema = pyspark.sql.types.StructType(
    [pyspark.sql.types.StructField("crim", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("zn", pyspark.sql.types.IntegerType(), False),
     pyspark.sql.types.StructField("indus", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("chas", pyspark.sql.types.IntegerType(), False),
     pyspark.sql.types.StructField("nox", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("rm", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("age", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("dis", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("rad", pyspark.sql.types.IntegerType(), False),
     pyspark.sql.types.StructField("tax", pyspark.sql.types.IntegerType(), False),
     pyspark.sql.types.StructField("ptratio", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("b", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("lstat", pyspark.sql.types.FloatType(), False),
     pyspark.sql.types.StructField("medv", pyspark.sql.types.FloatType(), False)

     ])
