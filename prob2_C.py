from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
sc.setLogLevel("OFF")

parquetFile = sqlContext.read.parquet("empsal1")
parquetFile.registerTempTable("temp")

emp = sqlContext.sql("Select * from temp")
emp.show()

sqlContext.dropTempTable("temp")

sc.stop()
