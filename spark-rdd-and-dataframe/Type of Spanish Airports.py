# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 17:10:32 2021

@author: pablo
"""
from pyspark import SparkConf, SparkContext

def main() -> None:

    ruta = "D:/Dropbox/Pablo/Master/Modulo 9. Scalable Data Processing/Java/Data/airports.csv"
      
    spark_conf = SparkConf()\
        .setAppName("AirportPy")\
        .setMaster("local[2]")

    
    spark_context = SparkContext(conf=spark_conf)
    
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    iso_country = "\"ES\""

    output = spark_context\
        .textFile(ruta)\
        .map(lambda line: line.split(','))\
        .filter(lambda array : array[8]==iso_country)\
        .map(lambda array: (array[2], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda pair: pair[1], ascending=False)

    f=open("outputAirport","w")
      
    for (name, num) in output.collect():
         f.write(str(name)+" : "+str(num)+"\n")
         print(str(name)+" : "+str(num))

    f.close()

    spark_context.stop()
        
if __name__ == "__main__":
    """
    Python program that uses Apache Spark to sum a list of numbers stored in files
    """
    main()

