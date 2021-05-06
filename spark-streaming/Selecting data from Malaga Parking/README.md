# Brief explanation of the application.

Using the data produced by the file "Downloading data of Malaga Parking.py", I have done a PySpark application that selects the data of each parking only when the parking has a capacity higher than zero. The result must be updated each minute. The outcoming data frames have to contain the following fields: name, capacity and number of available slots (free slots).

Both scripts must be run simultaneously, and it is also necessary to update the paths where the sensor temperature files will be created and read.
