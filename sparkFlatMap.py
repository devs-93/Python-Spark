from pyspark import SparkContext, SparkConf


##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("SparkflatMap").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

##############################################################################
########################Flat Map Transformation###############################
##############################################################################
data=sc.parallelize([2,4,5,6,7])
aft_flat_map=data.flatMap(lambda x:[x,x,x,x,x]).collect()
print(aft_flat_map)

##############################################################################
#########################Map Map Transformation###############################
##############################################################################
data=sc.parallelize([2,4,5,6,7])
aft_map=data.map(lambda x:[x,x,x,x,x]).collect()
print(aft_map)
