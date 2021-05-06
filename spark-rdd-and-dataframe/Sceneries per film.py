# -*- coding: utf-8 -*-
"""
Created on Wed Mar 17 00:16:33 2021

@author: pablo
"""

from pyspark import SparkConf, SparkContext

def main() -> None:

    ruta = "D:/Dropbox/Pablo/Master/Modulo 9. Scalable Data Processing/Java/Data/Film_Locations_in_San_Francisco.csv"
       
    spark_conf = SparkConf()\
        .setAppName("Films_LocationsPython")\
        .setMaster("local[2]")

    spark_context = SparkContext(conf=spark_conf)


    films = spark_context\
        .textFile(ruta)\
        .map(lambda line: line.split(','))\
        .map(lambda array: (array[0],1) )\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda pair: pair[1], ascending=False)
    
    total = 0
    escenarios=0
    for (peli,num_escenarios) in films.collect():
        total+=1
        escenarios+=int(num_escenarios)
   
    output=films.filter(lambda pair : pair[1]>=20)

    f=open("salidaFilms","w")
     
    for (peli, count) in output.collect():
          print("("+peli+", "+str(count)+")")  
          f.write("("+peli+", "+str(count)+")"+"\n")

    print("Total number of films: "+str(total))
    print("Average of number of scenaries per film: "+str(escenarios/total))

    f.write("Total number of films: "+str(total)+"\n")
    f.write("Average of number of scenaries per film: "+str(escenarios/total))
    f.close()

    spark_context.stop()
        
if __name__ == "__main__":
    """
    Python program that uses Apache Spark to sum a list of numbers stored in files
    """
    main()

