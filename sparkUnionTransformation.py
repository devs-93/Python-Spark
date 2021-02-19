from pyspark import SparkConf,SparkContext


##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

##############################################################################
data1=sc.parallelize(range(1,10))
data2=sc.parallelize(range(9,11))
datafinal=data1.union(data2)
res=datafinal.collect()
print(res)