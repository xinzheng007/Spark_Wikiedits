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
	yield (genre, pages) 


def reducefunc(key, values):
	yield (key, min(values))


runtask(documents, mapfunc, reducefunc)

