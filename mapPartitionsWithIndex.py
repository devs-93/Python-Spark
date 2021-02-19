from itertools import product
from pyspark import SparkContext, SparkConf



conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")