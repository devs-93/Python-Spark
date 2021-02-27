from awsS3DataCrawlerSparkPython_v3 import SparkConf, SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = sc.parallelize(range(1,21),1)
zeroValue=0
data2= data.fold(zeroValue,lambda acc,element : acc+1 if acc>element else element)
print(data.getNumPartitions())
print(data2)