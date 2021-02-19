from pyspark import SparkConf, SparkContext

##############################################################################
########################Spark Context Create##################################
##############################################################################
conf = SparkConf().setAppName("MapPartwithIndex").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

student_rdd = sc.parallelize([
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
    ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
    ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
    ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
    ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75),
    ("Jackeline", "Biology", 83),
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)], 4)


# Defining Seqencial Operation and Combiner Operations
# Sequence operation : Finding Maximum Marks from a single partition
def seq_op(accumulator, element):
    print('.........................')
    print(accumulator)
    print(element)
    print('#########################')
    if(accumulator > element[1]):
        return accumulator
    else:
        return element[1]


# Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def comb_op(accumulator1, accumulator2):
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    print(accumulator1)
    print(accumulator2)
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    if(accumulator1 > accumulator2):
        return accumulator1
    else:
        return accumulator2

# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
zero_val = 0
aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2])))
print(aggr_rdd.collect())
aggregated_data=aggr_rdd.aggregateByKey(0, seq_op, comb_op)

# Check the Outout
for tpl in aggregated_data.collect():
    print(tpl)



