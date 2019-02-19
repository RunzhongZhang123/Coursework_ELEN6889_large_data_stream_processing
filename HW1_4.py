from pyspark import SparkContext
sc = SparkContext()

data = sc.textFile('epa-http.txt').map(lambda x: (x.replace('.',' ').split(' ')[0], x.replace('.',' ').split(' ')[1], x.replace('.',' ').split(' ')[2], x.replace('.',' ').split(' ')[len(x.replace('.',' ').split(' ')) - 1]))
#print(data.take(15))
data = data.filter(lambda x: x[3] != '-')
data1 = data.filter(lambda x: x[0].isdigit() == True and x[1].isdigit() == True and x[2].isdigit() == True)
#print(data.take(15))
data1 = data1.map(lambda x: (str(x[0]) + '.' + str(x[1]) + '.' + str(x[2]), x[3]))
#print(data.take(15))
data1 = data1.reduceByKey(lambda x, y: int(x) + int(y))
print(data1.take(5))

data2 = data.filter(lambda x: x[0].isdigit() == False or x[1].isdigit() == False or x[2].isdigit() == False)
data2 = data2.map(lambda x: (str(x[0]) + '.' + str(x[1]), x[3]))
data2 = data2.reduceByKey(lambda x, y: int(x) + int(y))
print(data2.take(5))