#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # conf = SparkConf().setAppName("word count").setMaster("local[3]")
    # sc = SparkContext(conf = conf)
    spark = SparkSession.builder \
            .appName("Word COunt")\
            .master("local[3]")\
            .getOrCreate()
    
    lines = spark.sparkContext.textFile("in/word_count.text")
    
    words = lines.flatMap(lambda line: line.split(" "))
    
    wordCounts = words.countByValue()
    
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))

