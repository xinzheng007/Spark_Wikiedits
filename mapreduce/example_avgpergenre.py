from map_reduce import runtask

documents = [ 
		('drama', 200), ('education', 100), 
		('action', 20), ('thriller', 20),
		('drama', 220), ('education', 150), 
		('action', 10), ('thriller', 160),
		('drama', 140), ('education', 160), 
		('action', 20), ('thriller', 30) ]


def mapfunc(value):
	genre, pages = value
	yield (genre, (pages, 1)) 


def reducefunc(key, values):
	sum, newcount = 0, 0
	for (pages, count) in values:
		sum = sum + pages * count
		newcount = newcount + count
	yield (key, (sum/newcount, newcount))


runtask(documents, mapfunc, reducefunc)

