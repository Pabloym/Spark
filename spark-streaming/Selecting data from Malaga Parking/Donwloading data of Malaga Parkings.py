import sys
import time
import datetime
import random
import os
import requests

directory = "D:\Dropbox\Pablo\Master\Modulo 11. Scalable Data processing. Machine Learning and Streaming\Practicas\MalagaParking"

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

urlToFetch = "https://datosabiertos.malaga.eu/recursos/aparcamientos/ocupappublicosmun/ocupappublicosmun.csv"
streamingDirectory = directory
secondsToSleep = 60
n=1
while True:
    time_stamp = str(time.time())

    newFile = streamingDirectory + "/" + str(time_stamp).replace(".","") + ".csv"
    response = requests.get(urlToFetch, headers=headers)
    data = response.content.decode("utf-8")

    with open(os.path.join(streamingDirectory, newFile), "w") as file1:
        file1.write(data)

    print("A new file has been downloaded, total: "+str(n))

    n+=1
    time.sleep(secondsToSleep)
