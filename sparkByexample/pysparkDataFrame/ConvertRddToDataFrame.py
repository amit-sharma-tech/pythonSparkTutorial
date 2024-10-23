from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("convert RDD to Data Frame").master("local[*]").getOrCreate()

dept =  [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
#print(rdd.first())

#rdd.toDF()
# df = rdd.toDF()
# df.printSchema()
# df.show(truncate=False)

#deptColumns = ["dept_name","dept_id"]

# df2 = rdd.toDF(deptColumns)
# df2.printSchema()
# df2.show(truncate=False)

# deptDf = spark.createDataFrame(rdd,schema=deptColumns)
# deptDf.printSchema()
# deptDf.show(truncate=False)

from pyspark.sql.types import StructField,StructType,StringType

deptSchema = StructType([
    StructField("dept_name",StringType(),True),
    StructField("dept_id",StringType(),True)
])

deptDf1 = spark.createDataFrame(rdd,schema = deptSchema)
deptDf1.printSchema()
deptDf1.show(truncate=False)