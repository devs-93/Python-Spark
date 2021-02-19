from pyspark import SparkConf, SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

rddNumList = sc.parallelize(
    list(
        [
            ("cs", ("jack", 14000.0)),
            ("cs", ("bron", 1200.0)),
            ("phy", ("sam", 2200.0)),
            ("phy", ("ronaldo", 500.0))
     ]
     ))
zerovalue=("dummy", 0.0)
maxByDept =rddNumList.foldByKey(
    zerovalue , lambda acq,element : acq if int(acq[1])>int(element[1]) else element
                                )
data = maxByDept.collect()
print(data)