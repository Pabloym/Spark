# Brief explanation of each file and URL to download data files.

## Analysis of access to electricity.

In this exercise we will focus on the percentage of population having access to electricity in all the countries of the world from 1960 to 2014.  
The exercise consist in, given a range of years (e.g., 1970 and 1980), prints on the screen the average number of countries of each of the regions (South Asia, North America, etc.) having at least 99% of electricity for all the years between the indicated range. The output must be ordered in descendent order by the percentage value.

The data can be obtained from this link:

URL: https://data.worldbank.org/indicator/EG.ELC.ACCS.ZS

After selecting the CSV format you will get a zip file containing, among others, these two files: API_EG.ELC.ACCS.ZS_DS2_en_csv_v2.csv and Metadata_Country_API_EG.ELC.ACCS.ZS_DS2_en_csv_v2.csv


## Analysis of Tweets

Taking as data source the file named "tweets.tsv", which contains tweets written during a game of the USA universitary basketball league, the exercise consists on the following tasks:

1. Search for the 10 most repeated words in the matter field of the tweets and prints on the screen the words and their frequency, ordered by frequency. Common words such as "RT", "http", etc, should be filtered.

2. Find the user how has written the mosts tweets and print the user name and the number of tweets

3. Find the shortest tweet emitted by any user. The program will print in the screen the user name, the length of the tweet, and the time and date of the tweet.


## Dealing with unclean data

This task consists in developing a Jupyter notebook with PySpark to read the file, remove all the invalid lines and remove those that are appears more than one time, and plot the clean data.

I have used the data file named "bridge.csv".

## Large Airports

This task is intended to find the 10 countries having more airports of the "large" category, displaying on the screen the country name and the number of airports, ordered in descending order by the number of airports.

URLs: https://ourairports.com/data/airports.csv    //    https://ourairports.com/data/countries.csv

## Scenaries per film

This exercise is about developing a RDD-based Spark program in Python that finds those films with 20 or more locations and print them on the screen. Furthermore, the program must print the number of films and the average of locations per flim.

URL: https://data.sfgov.org/Culture-and-Recreation/Film-Locations-in-San-Francisco/yitu-d5am

## Spanish Airports

This task consist in develop an application to read the airports.csv file and counts the number of spanish airports for each of the four types, writing the results in a text file.

URL: http://ourairports.com/data/airports.csv
