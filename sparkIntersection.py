from awsS3DataCrawlerSparkPython_v3 import SparkContext,SparkConf
##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

###############################################################################
###################Spark Intersection #########################################
###############################################################################

data1=sc.parallelize(range(1,10))
data2=sc.parallelize(range(5,10))
finaldata=data1.intersection(data2).collect()
print(finaldata)
