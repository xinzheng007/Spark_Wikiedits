from dummy_spark import SparkContext, SparkConf
from random import randint

sconf = SparkConf()
sc = SparkContext(master='', conf=sconf)

rdd = sc.parallelize(list( [(x, y, randint(10, 20)) for x in range(1, 31) for y in ['BRU', 'LEU', 'ANT']] ))

print(rdd)

# Get maximum per location over all days

result = rdd.map( lambda item: (item[1], item[2]) )\
	.reduceByKey(lambda a, b: max(a,b))

print('Result = ', result)