from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
##############################################################################
# ########################Spark Context Create##################################
# ################# #############################################################
# conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
# sc = SparkContext(conf=conf)
# sc.setLogLevel("WARN")
#
#
# baby_names = sc.textFile("/home/devbrt.shukla/Desktop/scalaoutput/Baby_Names__Beginning_2007.csv",10)
# # baby_names.repartition(10).saveAsTextFile('/home/devbrt.shukla/Desktop/scalaoutput/output/repartition/')
# # baby_names.coalesce(10).saveAsTextFile('/home/devbrt.shukla/Desktop/scalaoutput/output/colease/')
# print(type(baby_names))
#
spark = SparkSession.builder.appName("Detecting-Malicious-URL App").getOrCreate()
df=spark.read.format("csv").option('header','true').option('inferSchema','true').option('path','/home/devbrt.shukla/Desktop/scalaoutput/Baby_Names__Beginning_2007.csv').load()
df1=df.repartition(5)
print(df1.rdd.getNumPartitions())
df1.foreachPartition(lambda x : print('\n','No of Count Per Partiotion ---->',len(list(x))))

