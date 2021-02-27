from awsS3DataCrawlerSparkPython_v3 import SparkContext, SparkConf


conf = SparkConf().setAppName("firstAppInPythonSpark").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

rows = distData.map(lambda x: x*x*x*x)
for i in rows.collect():
    print(i)