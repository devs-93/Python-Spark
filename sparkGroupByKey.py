from awsS3DataCrawlerSparkPython_v3 import SparkConf,SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

##############################################################################
########################Spark Tranformation###################################
##############################################################################

baby_names=sc.textFile("/home/devbrt.shukla/Desktop/scalaoutput/Baby_Names__Beginning_2007.csv")
rows=baby_names.map(lambda x : str(x).split(","))
namesToCounties = rows.map(lambda n: (str(n[1]),str(n[2])))
namesToCounties=namesToCounties.groupByKey()
final_result=namesToCounties.map(lambda x : {x[0]: list(x[1])})
print(final_result.take(2))