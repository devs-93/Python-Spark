from pyspark import SparkConf,SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

##############################################################################
########################Spark Distinct Data ##################################
##############################################################################
data1=sc.parallelize(range(1,10))
data2=sc.parallelize(range(5,10))
finaldata=data1.union(data2).distinct().collect()
print(finaldata)