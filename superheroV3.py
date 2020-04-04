# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 15:51:40 2020

@author: andre 
"""
from pyspark.sql import SparkSession
from pyspark.sql import Row
# from pyspark.sql import functions

def loadsuperheroName():
    superheroName = {}
    with open("MarvelNames.txt") as f:
        for line in f:
            fields = line.split('\"')
            superheroName[int(fields[0])] = fields[1].encode("utf8")
    return superheroName


def countCoOccurences(line):
    elements = line.split()
    return Row(heroid = int(elements[0]) , numfriends = len(elements) - 1)  #numFriends = len(elements) - 1 

nameDict = loadsuperheroName()

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SuperHeroCouncil").getOrCreate()

# Get the raw data using the sparkContext command
lines = spark.sparkContext.textFile("file:///SparkCourse/MarvelGraph.txt")  # file with superheroID, length friends

"""  Step 2:
Convert text file (lines) into RDD using function above. This creates heroid, numfriends columns. heroid pass to loadsuperheroName() function so that it automatically pulls the name so dont have to do a for loop below to print. This replaces doing the map() function 
https://stackoverflow.com/questions/48111066/how-to-convert-pyspark-rdd-pipelinedrdd-to-data-frame-with-out-using-collect-m/48111699 """

heroes = lines.map(countCoOccurences) #map creates heroid, numfriends for each Row
#flatMap flattens the heroid and count of their friends into 1 list: [5988, 48, 5989, 40, 5982, 42]
 
# print(type(heroes), "type of heroes")
# heroes.collect()

# Convert that RDD to a DataFrame called superheroDataset
superheroDataset = spark.createDataFrame(heroes).cache()  
superheroDataset.createOrReplaceTempView("heroes")   # make heroes database. Need this or else can't query database
# print("superhero dataset is of type:", type(superheroDataset))
# superheroDataset.take(100)
# print(superheroDataset.show())


# SQL can be run over DataFrames that have been registered as a table.
# topHeroIDs = spark.sql("SELECT heroid, numfriends FROM heroes ORDER By numfriends desc ")   # 

topHeroIDs = superheroDataset.select(superheroDataset["heroid"], superheroDataset["numfriends"])
topHeroIDs = superheroDataset.sort("numfriends", ascending = False)

topHeroIDs.show()
# Show the results at this point:
#topheroes = topHeroIDs.take(10)
# print(topheroes)
#topheroes.show()

"""
# print the results
print("\n")
for result in topheroes:
    print("%s: %d" % (nameDict[result[0]], result[1]))
"""
    
# Stop the session
spark.stop()

# !spark-submit superheroV3.py  > superheroV5.txt