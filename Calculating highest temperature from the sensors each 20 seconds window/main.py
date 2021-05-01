# -*- coding: utf-8 -*-
"""
Created on Fri Apr 23 00:14:56 2021

@author: pablo
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import window

directory = "D:\Dropbox\Pablo\Master\Modulo 11. Scalable Data processing. Machine Learning and Streaming\Practicas\csv"

spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("Highest temperature each 20 seconds") \
        .getOrCreate()

fields = [StructField("sensor", StringType(), True),
              StructField("value", DoubleType(), True),
              StructField("timestamp", TimestampType(), True)]

windows_size = 20
slide_size = 10

window_duration = '{} seconds'.format(windows_size)
slide_duration = '{} seconds'.format(slide_size)


# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("csv") \
    .schema(StructType(fields)) \
    .options(header='false') \
    .load(directory)


lines.printSchema()


# Compute the maximum temperature

values = lines.groupBy(
    window(lines.timestamp, window_duration, slide_duration),"sensor")\
    .agg(functions.max("value").alias("maximum"))\
    .orderBy("sensor")


values.printSchema()

# Start running the query that prints the output in the screen
query = values \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


query.awaitTermination()
