from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,StructField

spark = SparkSession.builder.appName("Empty data frame").master("local[*]").getOrCreate()

#Create Empty RDD

emptyRdd = spark.sparkContext.emptyRDD()
# print(emptyRdd)

#Creates Empty RDD using parallelize
# rdd2= spark.sparkContext.parallelize([])
# print(rdd2)


#create Schema

schema = StructType([
    StructField("firstName",StringType(),True),
    StructField("middleName",StringType(),True),
    StructField("lastName",StringType(),True)
])

df = spark.createDataFrame(emptyRdd,schema)
#df.printSchema()

#convert empty RDD to Dataframe

df1 = emptyRdd.toDF(schema)
df1.printSchema()