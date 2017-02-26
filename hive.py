from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext
conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)

hive = HiveContext(sc)
dfs = hive.sql("select addresses from customers")

print dfs.groupBy("addresses.shipping.state").count().show()

sc.stop()
