from itertools import product
from awsS3DataCrawlerSparkPython_v3 import SparkContext, SparkConf



conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")