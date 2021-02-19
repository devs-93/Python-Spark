from pyspark import SparkConf, SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = sc.parallelize([('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)])
data12=data.sortByKey(False)
print(data12.collect())
data14=data.sortByKey(True)
print(data14.collect())
data15=data.sortByKey()
print(data15.collect())