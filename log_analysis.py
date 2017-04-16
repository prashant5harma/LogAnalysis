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


Q 2
counts=sc.textFile("testt.txt").filter(lambda line: "starting session" in line).filter(lambda line: "achille" in line)

counts=sc.textFile(input_text_file).filter(lambda line: "starting session" in line).flatMap(lambda x: x.split()[-1]).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)




Q 3
counts=sc.textFile("testt.txt").filter(lambda line: "starting session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct()



Q 5

from pyspark.sql import Row
from pyspark.sql.functions import *
text_file = sc.textFile("testt.txt")
df = text_file.map(lambda r: Row(r)).toDF(["line"])
df.show()
df.filter(lower(df['line']).rlike("qwrwret") ).count()


Q 7

count1=sc.textFile("testt.txt").filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1])

count2=sc.textFile("testt.txt").filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1])

count1.intersection(count2).collect()
