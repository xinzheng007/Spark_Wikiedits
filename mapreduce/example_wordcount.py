from map_reduce import runtask

documents = ['one two three three four three six',
	  'five six one one one two three',
	  'six two three four six']


# Provide a mapping function of the form mapfunc(value)
# Must yield one or more (k,v) pairs
def mapfunc(value):
	for x in value.split():
		yield (x,1) 


# Provide a reduce function of the form reducefunc(key, values)
# Must yield a (k,v) pair with same structure
def reducefunc(key, values):
	yield (key, sum(values))


# Pass your input list, mapping and reduce functions
runtask(documents, mapfunc, reducefunc)
