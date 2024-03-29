import sys
sys.path.insert(0, '.')
#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    # conf = SparkConf().setAppName("airports").setMaster("local[2]")
    # sc = SparkContext(conf = conf)

    spark = SparkSession.builder \
            .appName("Airports example")\
            .master("local[3]")\
            .getOrCreate()
    
    airports = spark.sparkContext.textFile("in/airports.text")

    airportsInUSA = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    
    airportsNameAndCityNames = airportsInUSA.map(splitComma)

    airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")