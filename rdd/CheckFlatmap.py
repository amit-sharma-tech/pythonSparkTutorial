from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("FlatMap Example").setMaster("local[3]")
    sc = SparkContext(conf = conf)

    data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
    
    rdd = sc.parallelize(data)

    # for elem in rdd.collect():
    #     print(elem)

    rdd2 = rdd.flatMap(lambda x : x.split(" "))
    for ele in rdd2.collect():
        print(ele)