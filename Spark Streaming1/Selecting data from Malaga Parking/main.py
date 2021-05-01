import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import window, current_timestamp

directory = "D:\Dropbox\Pablo\Master\Modulo 11. Scalable Data processing. Machine Learning and Streaming\Practicas\MalagaParking"
spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("Malaga Parking Updates") \
    .getOrCreate()

windows_size = 60
slide_size = 60

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
        StructField("fechahora_ultima_actualizacion", IntegerType()),
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


values = lines \
    .withColumn("fechahora_ultima_actualizacion",current_timestamp())\
    .select(lines["nombre"],lines["capacidad"],lines["libres"])\
    .filter(lines.capacidad>0)
    #.groupBy(window(lines.fechahora_ultima_actualizacion, window_duration, slide_duration))

query = values \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()

