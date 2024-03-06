from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Eample of map").master("local[*]").getOrCreate()

fileName = "in/dummy.txt"

fileread = spark.sparkContext.textFile(fileName)

#mapExample  = fileread.map(lambda l : l.split(" "))

# print(mapExample.collect())

words = fileread.flatMap(lambda l : l.split(" "))
wordCounts = words.countByValue()

#print(wordCounts)

for word,count in wordCounts.items():
    print(" {} : {}" .format(word,count))