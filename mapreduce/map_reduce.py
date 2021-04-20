# Simple map-reduce simulation in Python
# author: Seppe vanden Broucke

def map(input, mapfunc):
	# This loop could be executed over multiple machines
	for i in input:
		for x in mapfunc(i):
			yield x 


def reduce(input, reducefunc):
	# Reduce per key
	for key in set([kv[0] for kv in input]):
		values = [kv[1] for kv in input if kv[0] == key]
		for x in reducefunc(key, values):
			yield x


def should_reduce(input):
	# Are there still keys to be reduced?
	for key in set([kv[0] for kv in input]):
		values = [kv[1] for kv in input if kv[0] == key]
		if len(values) > 1:
			return True
	return False


def runtask(input_list, mapfunc, reducefunc, reduce_after_nr_elements=3):
	elements = []
	for mapped in map(input_list, mapfunc):
		elements.append(mapped)
		print('\n* Got mapped result:', mapped)
		print('  Elements is now', elements)
		# Simulate reduce working in parallel
		if len(elements) > 3 and should_reduce(elements):
			newelements = [x for x in reduce(elements, reducefunc)]
			print('\n* Reduced', elements)
			print('  To new elements', newelements)
			elements = newelements			
	while should_reduce(elements):
		elements = [x for x in reduce(elements, reducefunc)]
		print('\n* Reduced', elements)
		print('  To new elements', newelements)
	print('\n\n* Final output: ', elements)
	return elements


if __name__ == '__main__':
	print("Don't run this file, import it in another file")
	print("Or run one of the examples")