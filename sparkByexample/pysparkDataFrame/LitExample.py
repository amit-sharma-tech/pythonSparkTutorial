from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

spark = SparkSession.builder.appName("convert RDD to Data Frame").master("local[*]").getOrCreate()

data = [
    ("James",23),
    ("Ann",40)
]

df = spark.createDataFrame(data).toDF("name.fname","gender")

#df.printSchema()

#using dataframe object
# df.select(df.gender).show()
# df.select(df['gender']).show()

# df.select(df["`name.fname`"]).show()

# #using  SQL col() function

# from pyspark.sql.functions import col

# df.select(col("gender")).show()

# df.select(col("`name.fname`")).show()

from pyspark.sql import Row
from pyspark.sql.functions import col

data= [
    Row(name="James",prop=Row(hair="black",eye="blue")),
    Row(name="Ann",prop=Row(hair="grey",eye="black"))
]

df = spark.createDataFrame(data)
df.printSchema()

df.select(df.prop.hair).show()
df.select(df["prop.hair"]).show()

df.select(col("prop.hair")).show()

df.select(col("prop.*")).show()
