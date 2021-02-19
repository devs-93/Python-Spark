from pyspark import SparkConf, SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

rddNumList = sc.parallelize(range(1,10),1)
rddNumList2=rddNumList
data3=rddNumList2.fold(0,(lambda x,y : x+y))
# data3=rddNumList2.fold(0,(lambda x,y : x+y))
print(data3)