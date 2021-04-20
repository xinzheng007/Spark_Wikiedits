from dummy_spark import SparkContext, SparkConf

sconf = SparkConf()
sc = SparkContext(master='', conf=sconf)

rdd = sc.parallelize(list(range(1, 21)))
print(rdd.filter(lambda x: x % 3 == 0))


