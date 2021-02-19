from pyspark import SparkConf, SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

##############################################################################
########################Spark Tranformation###################################
##############################################################################

# baby_names = sc.textFile("/home/devbrt.shukla/Desktop/scalaoutput/Baby_Names__Beginning_2007.csv")
# filtered_rows = baby_names.filter(lambda line: "Count" not in line)
# filtered_map_rows = filtered_rows.map(lambda data: str(data).split(','))
# finaldata1 = filtered_map_rows.map(lambda x: ( str(x[1]), int(x[4]) ))
# # finaldata3 = finaldata1.reduceByKey(lambda x, y: x + y)
# finaldata3 = finaldata1.groupByKey().collect()
# print(finaldata3)

data = sc.parallelize([('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)])
group = data.groupByKey()
reducedata = group.map(lambda x: (x[0],sum(list(x[1]))))
print(reducedata.collect())

data = sc.parallelize([('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)])
group12 = data.reduceByKey(lambda x ,y : print(x,y))
# reducedata = group.map(lambda x: (x[0],sum(list(x[1]))))
print(group12.collect())
