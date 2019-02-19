from pyspark import SparkContext
sc = SparkContext()
data = sc.textFile("epa-http.txt").map(lambda x: (x.split(' ')[0], x.split(' ')[len(x.split(' ')) - 1]))
#print(data.take(15))
data = data.filter(lambda x: x[1] != '-')
#print(data.take(15))
data = data.reduceByKey(lambda x, y: int(x) + int(y))
data = data.sortBy(lambda x: -int(x[1]))
print(data.take(15))