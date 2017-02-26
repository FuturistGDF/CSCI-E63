from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
sc.setLogLevel("OFF")
emps = sc.textFile("emps-1")
emps_fields = emps.map(lambda e: e.split(","))

employees = emps_fields.map(lambda e: Row(name = e[0], age = int(e[1]), salary = float(e[2])))
employeeDF = sqlContext.createDataFrame(employees)

employeeDF.printSchema()

sc.stop()
