from map_reduce import runtask

documents = [ 
		('drama', 2016, 200), ('drama', 2016, 100), 
		('action', 2016, 20), ('action', 2016, 20),
		('drama', 2015, 220), ('drama', 2015, 150), 
		('action', 2015, 10), ('action', 2015, 160),
		('drama', 2014, 140), ('drama', 2014, 160), 
		('action', 2014, 20), ('action', 2014, 30) ]



def mapfunc(value):
	genre, year, pages = value
	yield ( genre + '_' + str(year), (pages, pages) )


def reducefunc(key, values):
	minimum = min([x[0] for x in values])
	maximum = max([x[1] for x in values])
	yield ( key, (minimum, maximum) )


runtask(documents, mapfunc, reducefunc)

