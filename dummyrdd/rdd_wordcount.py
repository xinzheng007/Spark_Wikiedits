from dummy_spark import SparkContext, SparkConf
from lxml import html
import urllib.request
import tempfile

sconf = SparkConf()
sc = SparkContext(master='', conf=sconf)

temp = tempfile.NamedTemporaryFile(delete=False)
html_text = urllib.request.urlopen('https://en.wikipedia.org/wiki/Apache_Spark').read().decode('utf-8')
doc = html.fromstring(html_text)
temp.write(doc.text_content().encode('ascii', 'ignore'))
temp.close()

text_file = sc.textFile(temp.name)

# flatMap flattens the list resulting from a map
# reduceByKey takes two a and b values per key and outputs a new (key, f(a,b)) result
counts = text_file \
	.flatMap(lambda line: line.split(" ")) \
	.map(lambda word: (word.strip(), 1)) \
	.reduceByKey(lambda a, b: a + b) \
	.filter(lambda x: x[1] > 10)

print(counts)

