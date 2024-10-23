from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder.appName("convert RDD to Data Frame").master("local[*]").getOrCreate()

structData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
]

structSchema = StructType([
    StructField("name",StructType([
        StructField("firstName",StringType(),True),
        StructField("lastName",StringType(),True),
        StructField("lastName",StringType(),True)
    ])),
    StructField("id",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("salary",IntegerType(),True)
])

df = spark.createDataFrame(data = structData,schema = structSchema)
# df.printSchema()
# df.show(truncate=False)

#updating existing structtype using struct

# from pyspark.sql.functions import col,when,struct

# updateDF = df.withColumn("otherInfo",
#                         struct(col("id").alias("indentifier"),  
#                             col("gender").alias("gender"),
#                             col("salary").alias("salary"),
#                             when(col("salary").cast(IntegerType()) <  2000 ,"Low")
#                                 .when(col("salary").cast(IntegerType()) < 4000, "Medium")
#                                 .otherwise("High").alias("Salary_Grade")
#                             )
#                         ).drop("id","gender","salary")
# updateDF.printSchema()
# updateDF.show(truncate=False)

