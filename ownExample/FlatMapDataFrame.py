from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Flat Map example with data Frame").getOrCreate()

arrayData = [
    ('james',['java','scala'],{'hair':'black','eye':'brown'}),
    ('michel',['spark','java',None],{'hair':'brown','eye':None}),
    ('Robert',['CSharp',''],{'hair':'red','eye':''}),
    ('Washington',None,None),
    ('Jefferson',['1','2'],{})
]
    
df = spark.createDataFrame(data=arrayData,schema=['name','knowlanguage','properties'])

from pyspark.sql.functions import explode

#df2 = df.select(df.name,explode(df.knowlanguage))
df2 = df.select(df.name)
df2.printSchema()
#df2.show()
