from pyspark import SparkConf,SparkContext


##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("SparkflatMap").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

##############################################################################
########################Filter Transformation#################################
##############################################################################
data=sc.parallelize([2,4,5,6,7,10,1,12,14,20])
filtered_data=data.filter(lambda x : (x%2==0))
final_result=filtered_data.collect()
print(final_result)
