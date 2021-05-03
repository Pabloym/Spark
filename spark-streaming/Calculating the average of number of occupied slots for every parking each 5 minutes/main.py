import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import window, current_timestamp

directory = "D:\Dropbox\Pablo\Master\Modulo 11. Scalable Data processing. Machine Learning and Streaming\Practicas\MalagaParking"
spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("Malaga Parking Updates") \
    .getOrCreate()

windows_size = 300
slide_size = 300

window_duration = '{} seconds'.format(windows_size)
slide_duration = '{} seconds'.format(slide_size)

fields = StructType([
        StructField("poiID", StringType()),
        StructField("nombre", StringType()),
        StructField("direccion", StringType()),
        StructField("telefono", StringType()),
        StructField("correoelectronico", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("altitud", DoubleType()),
        StructField("capacidad", IntegerType()),
        StructField("capacidad_discapacitados", IntegerType()),
        StructField("fechahora_ultima_actualizacion", TimestampType()),
        StructField("libres", IntegerType()),
        StructField("libres_discapacitados", IntegerType()),
        StructField("nivelocupacion_naranja", StringType()),
        StructField("nivelocupacion_rojo", StringType()),
        StructField("smassa_sector_sare", StringType()),
])

lines = spark \
    .readStream \
    .format("csv") \
    .schema(StructType(fields)) \
    .options(header='true') \
    .load(directory)

lines.printSchema()

filtrado = lines\
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("ocupados", lines["capacidad"]-lines["libres"]) \
    .filter(lines.capacidad>0)

filtrado.printSchema()

values = filtrado \
    .select("nombre", "capacidad", "ocupados","timestamp") \
    .groupBy(window(filtrado["timestamp"], window_duration, slide_duration), "nombre","capacidad")\
    .agg(functions.mean("ocupados").alias("Average of occupied slots"))\
    .orderBy("nombre")


query = values \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

