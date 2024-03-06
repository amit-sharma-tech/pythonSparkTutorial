import sys
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf = SparkContext().setAppName("Find Lat data").setMaster("local[*]")
    sc = SparkConf(conf = conf)

    readFile = sc.textFile("in/airports.text")
    breakpoint()
