# Brief explanation of each task and URLs to download data files.

## 1. Clustering of Crimen Locations

Clustering of the locations (latitude, longitude) of the crime type having more incidences. After asking for the number of clusters and selecting one of the clustering algorithms in SparkML, the results will be shown in a 2D chart including the centroids and the number of points of each cluster. Furthermore, these information will be plotted on top of a map of Chicago, trying to show the data in the most useful way to the user.

URL: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2

## 2. Data Preprocessing and Crossvalidation

Steps on this task:

    1. Read the CSV inferring the scheme.
    2. Transforming the read dataframe into another one with the format required by SparkML.
    3. Select a classifier and train it using crossvalidation.
    4. Analyze the quality of the classifier.

URL: https://archive.ics.uci.edu/ml/datasets/Wine

## 3. Random Forest

Application that uses the random forest implementation of Spark to train and test a model (with a ratio 75%-25%) using a data file in libsvm format. The program prints as output the value of the accuracy metric.

I have downloaded the file breast-cancer_scale.

URL: https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#breast-cancer
