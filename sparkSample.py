from awsS3DataCrawlerSparkPython_v3 import *

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data=sc.parallelize(range(100,200))
data2=data.sample(True,.4).count()
print(data2)