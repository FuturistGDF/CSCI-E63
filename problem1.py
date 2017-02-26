from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)
sc.setLogLevel("OFF")
movieA = sc.textFile("movie1")
movieB = sc.textFile("movie2")
stopWords = sc.textFile("stopwords")

stopWordsCollected = stopWords.flatMap(lambda x: x.lower().split())

movie1RDD = (movieA
	.flatMap(lambda x: x.encode("ascii", "ignore").lower().split())
	.map(lambda x: (x,1))
	.reduceByKey(lambda x, y: x+ y)
         )

movie2RDD = (movieB
	.flatMap(lambda x: x.encode("ascii", "ignore").lower().split())
	.map(lambda x: (x,1))
	.reduceByKey(lambda x, y: x+ y)
	)

stopWordsRDD = (stopWords
		  .flatMap(lambda x: x.split())
		  .map(lambda x: (x,1))
		  .reduceByKey(lambda x, y: x+ y)
		  )

movie1FilterRDD = movie1RDD.subtractByKey(stopWordsRDD)
movie2FilterRDD = movie2RDD.subtractByKey(stopWordsRDD)

movie1Sort = movie1FilterRDD.takeOrdered(10, key = lambda x: -x[1])
movie2Sort = movie2FilterRDD.takeOrdered(10, key = lambda x: -x[1])

#Print results
print ("--------------")
print ("Top words in Movie1")
for (word, count) in movie1Sort:
        print("%s: %i" % (word.encode('utf-8'), count))
print ("--------------")

print ("Top words in Movie2")
for (word, count) in movie2Sort:
	print("%s: %i" % (word.encode('utf-8'), count))

print ("--------------")
print ("Common words and count") 
#RDD of common words in both
commonRDD = movie1FilterRDD.join(movie2FilterRDD)
print ("Common word count %i:" % commonRDD.count())

randomRDD = commonRDD.sample(1,0.04,1234).collect()

for x in randomRDD:
	print x

sc.stop()
