from itertools import product

from pyspark import SparkContext, SparkConf


##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


parallel = sc.parallelize(range(1,20),5)

def show(index, datalist):
    yield 'index: '+str(index)+" values: "+ str(datalist)

data=parallel.mapPartitionsWithIndex(show).collect()
print(data)