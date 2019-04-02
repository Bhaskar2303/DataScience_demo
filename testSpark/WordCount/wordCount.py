from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile(sys.argv[0])

wordCounts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(lambda x, y : x+y)
output = wordCounts.collect()

for(word, count ) in output:
    print("%s: %i" % (word, count))

sc.stop()