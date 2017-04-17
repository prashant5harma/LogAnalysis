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

Q 9
#Start
from pyspark.sql import Row
from pyspark.sql.functions import *

counts=sc.textFile("iliad").filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct()
#arr = counts.zipWithIndex().collect()
arr = counts.collect()
#reff = sc.parallelize(arr).map(lambda x:(x[0],"user-"+str(x[1])))

# to remove the full stop
arr = [word[:-1] for word in arr]

#Loop to find all words in arr and replace them
for word in arr:
    a = word
    b = "User" + str(arr.index(word))
    a
    b
    df.replace([a],[b])
#[ for word in arr]

#make a df of the text file
text_file = sc.textFile("iliad")
df = text_file.map(lambda r: Row(r)).toDF(["line"])

#write df to file
import os
cwd = os.getcwd()
fn = cwd + "/Anonymized_logs"
df.write.format("text").save(fn)
#End


#Not required
reff = sc.parallelize(arr).map(lambda x:(x[0],"user-"+ arr.index(arr[])))
reffDef = reff.map(lambda r: Row(r)).toDF(["users"]).show()

text_file = sc.textFile("iliad")
df = text_file.map(lambda r: Row(r)).toDF(["line"])
df.show()
