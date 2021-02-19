from pyspark import SparkConf, SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


def createCombiner(value):
    return value, 1


def mergeValue(acc, value):
    return acc[0] + value, acc[1] + 1


def mergeCombiners(acc1, acc2):
    return acc1[0] + acc2[0], acc1[1] + acc2[1]


student_rdd = sc.parallelize([
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
    ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
    ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
    ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
    ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75),
    ("Jackeline", "Biology", 83),
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)], 1)

student_rdd1 = student_rdd.map(lambda x: (x[0], x[2]))
student_rdd2 = student_rdd1.combineByKey(createCombiner, mergeValue, mergeCombiners)
data2 = student_rdd2.map(lambda x: ((x[1][0] / x[1][1]), x[0])).collect()
print(data2)
