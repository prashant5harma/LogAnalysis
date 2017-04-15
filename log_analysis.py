#!/usr/bin/env python
Q 1
import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("ass").setMaster("local")
sc = SparkContext(conf=conf)

input_text_file=sys.argv[1]
output_text_file=sys.argv[2]

counts=sc.textFile(input_text_file).flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

counts.saveAsTextFile(output_text_file)




Q 3
counts=sc.textFile("testt.txt").map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1])


Q 5

from pyspark.sql import Row
from pyspark.sql.functions import *
text_file = sc.textFile("testt.txt")
df = text_file.map(lambda r: Row(r)).toDF(["line"])
df.show()
df.filter(lower(df['line']).rlike("qwrwret") ).count()
