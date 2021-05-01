from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, DataType
from pyspark.sql.functions import window
import sys
import time
import datetime
import random

# =============================================================================
#  En esta parte es donde vamos a ir generando los ficheros para leer
# =============================================================================

seconds_to_sleep = 5;

directory = "D:\Dropbox\Pablo\Master\Modulo 11. Scalable Data processing. Machine Learning and Streaming\Practicas\csv"

sensor_identifier1 = str(1)
sensor_identifier2 = str(2)

windows_size = 20
slide_size = 10

window_duration = '{} seconds'.format(windows_size)
slide_duration = '{} seconds'.format(slide_size)

while True:
    # Get current time
    current_time = str(time.time())

    # Create a new file name with the time stamp
    file_name1 = directory + "/" + current_time + "s" + str(sensor_identifier1) + ".csv"
    file_name2 = directory + "/" + current_time + "s" + str(sensor_identifier2) + ".csv"

    # Get the temperature
    temperature_value1 = random.randrange(12.0, 20.0)
    temperature_value2 = random.randrange(18.0, 30.0)

    # Write the current temperature value in the file
    with (open(file_name1, "w")) as file:
        time_stamp = datetime.datetime.now();
        file.write(sensor_identifier1 + "," + str(temperature_value1) + "," + str(time_stamp))

    with (open(file_name2, "w")) as file:
        time_stamp = datetime.datetime.now();
        file.write(sensor_identifier2 + "," + str(temperature_value2) + "," + str(time_stamp))

    print("Temperature: " + str(temperature_value1) + "; Sensor: " + str(sensor_identifier1))
    print("Temperature: " + str(temperature_value2) + "; Sensor: " + str(sensor_identifier2))
    # Sleep for a few seconds
    time.sleep(seconds_to_sleep)

