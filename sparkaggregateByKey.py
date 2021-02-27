from awsS3DataCrawlerSparkPython_v3 import SparkConf, SparkContext


##############################################################################
########################Spark Context Create##################################
##############################################################################

conf = SparkConf().setAppName("MapPartwithIndex").setMaster("spark://192.168.1.10:7077" )
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

##############################################################################
########################Spark Tranformation###################################
##############################################################################

# baby_names = sc.textFile("/home/devbrt.shukla/Desktop/scalaoutput/Baby_Names__Beginning_2007.csv")
# filtered_rows = baby_names.filter(lambda line: "Count" not in line)
# filtered_map_rows = filtered_rows.map(lambda data: str(data).split(','))
# finaldata1 = filtered_map_rows.map(lambda x: ( x[1], int(x[4]) ))
# # finaldata3 = finaldata1.reduceByKey(lambda x, y: x + y)
# # finaldata3 = finaldata1.aggregateByKey(0, lambda k,v: k+int(v), lambda v,k: k+v)
# finaldata4 = finaldata1.aggregateByKey(0, lambda k,v: k+int(v), lambda v,k: k+v)
# # print(finaldata3.take(3))
# print(finaldata4.take(3))


premierRDD = sc.parallelize([("Arsenal", "2014–2015", 75), ("Arsenal", "2015–2016", 71), ("Arsenal", "2016–2017", 75), ("Arsenal", "2017–2018", 63),("Chelsea", "2014–2015", 87), ("Chelsea", "2015–2016", 50), ("Chelsea", "2016–2017", 93), ("Chelsea", "2017–2018", 70),("Liverpool", "2014–2015", 62), ("Liverpool", "2015–2016", 60), ("Liverpool", "2016–2017", 76), ("Liverpool", "2017–2018", 75),("M. City", "2014–2015", 79), ("M. City", "2015–2016", 66), ("M. City", "2016–2017", 78), ("M. City", "2017–2018", 100), ("M. United", "2014–2015", 70), ("M. United", "2015–2016", 66), ("M. United", "2016–2017", 69), ("M. United", "2017–2018", 81)],1)

# premierRDD = sc.parallelize([
#     ("Arsenal", "2014–2015", 75), ("Arsenal", "2015–2016", 71), ("Arsenal", "2016–2017", 75),
#     ("Arsenal", "2017–2018", 63),
#     ("Chelsea", "2014–2015", 87), ("Chelsea", "2015–2016", 50), ("Chelsea", "2016–2017", 93),
#     ("Chelsea", "2017–2018", 70),
#     ("Liverpool", "2014–2015", 62), ("Liverpool", "2015–2016", 60), ("Liverpool", "2016–2017", 76),
#     ("Liverpool", "2017–2018", 75),
#     ("M. City", "2014–2015", 79), ("M. City", "2015–2016", 66), ("M. City", "2016–2017", 78),
#     ("M. City", "2017–2018", 100),
#     ("M. United", "2014–2015", 70), ("M. United", "2015–2016", 66), ("M. United", "2016–2017", 69),
#     ("M. United", "2017–2018", 81)
# ])


#####################################################################
######################Max in All Find By This Spark Jobs#############
#####################################################################

# Sequence operation : Finding Maximum Marks from a single partition

def seq_op(accumulator, element):
    if(accumulator > element[1]):
        return accumulator
    else:
        return element[1]



def comb_op(accumulator1, accumulator2):
    if(accumulator1 > accumulator2):
        return accumulator1
    else:
        return accumulator2


#####################################################################
######################Find Sum of all by Spark Jobs #################
#####################################################################

def seqfunction(x,y):
    return (x[0] + y[1], x[1]+1)


def combfunction(x,y):
    return (x[0] + y[0], x[1] + y[1])



zero=(0,0)
premierMap = premierRDD.map(lambda x: (x[0], (x[1], x[2])))
premierMax = premierMap.aggregateByKey(zero, seqfunction, combfunction)


# premierMax1 = premierMap.aggregateByKey((4, 0), seqFunc, combFunc)
for i in premierMax.collect():
    print(i)
# for i in premierMax1.collect():
#     print(i)
