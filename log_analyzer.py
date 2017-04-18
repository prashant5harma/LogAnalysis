import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.functions import *
from collections import Counter
from pyspark.sql import SparkSession


q = sys.argv[2]
fr_hosts = sys.argv[3:]
v1 = sys.argv[3]
v2 = sys.argv[4]

conf = SparkConf().setAppName("ass").setMaster("local")
sc = SparkContext(conf=conf)

if q == str(1):
   print "* Q1: line counts"
   for word in fr_hosts:
       log = sc.textFile(word)
       cnt = log.count()
       res = "  +  " + word + ":  " + str(cnt)
       print res

elif q == str(2):
   print "* Q2: sessions of user achille"
   for word in fr_hosts:
       log = sc.textFile(word).filter(lambda line: "Starting Session" in line).filter(lambda line: "achille" in line)
       cnt = log.count()
       res = "  +  " + word + ":  " + str(cnt)
       print res

elif q == str(3):
   print "* Q3: unique user names"
   for word in fr_hosts:
       log = sc.textFile(word).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct().collect()
       log = [word[:-1] for word in log]
       res = "  +  " + word + ":  " + str(log)
       print res

elif q == str(4):
   print "* Q4: sessions per user"
   for word in fr_hosts:
       log = sc.textFile(word).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).collect()
       res = "  +  " + word + ":  " + str(log)
       print res


elif q == str(5):
    print "* Q5: number of errors"
    for word in fr_hosts:
        spark = SparkSession(sc)
        text_file = sc.textFile(word)
        #hasattr(text_file, "toDF")
        df = text_file.map(lambda r: Row(r)).toDF(["line"])
        cnt = df.filter(lower(df['line']).rlike("error") ).count()
        res = "  +  " + word + ":  " + str(cnt)
        print res

elif q == str(6):
   print "* Q6: 5 most frequent error messages"
   for word in fr_hosts:
       spark = SparkSession(sc)
       text_file = sc.textFile(word)
       #hasattr(text_file, "toDF")
       df = text_file.map(lambda r: Row(r)).toDF(["line"])
       temp = df.select(df.line.substr(16, 10000).alias("newline"))
       err = temp.filter(lower(temp['newline']).rlike("error"))
       arr = err.rdd.map(lambda x: x.newline).collect()
       counter = Counter(arr)
       print "  +  " + word + ":  "
       print("\t"+"\n\t".join(map(str, counter.most_common(5))))

elif q == str(7):
   print "* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts."
   count1=sc.textFile(v1).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1])
   count2=sc.textFile(v2).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1])
   cnt = count1.intersection(count2).collect()
   print " + : " + str(cnt)

elif q == str(8):
   print "* Q8: users who started a session on exactly one host, with host name."
   con1=sc.textFile(v1).filter(lambda line: "Starting Session" in line).map(lambda x: (v1, x.split()[::-1])).map(lambda x:(x[0],x[1][0]))
   con2=sc.textFile(v2).filter(lambda line: "Starting Session" in line).map(lambda x: (v2, x.split()[::-1])).map(lambda x:(x[0],x[1][0]))
   intersection = con1.intersection(con2)
   union = con1.union(con2)
   cnt = union.subtract(intersection).distinct().collect()
   print " + : " + str(cnt)


elif q == str(9):
   print "Question number 9"
   print q

else:
   print "INVALID QUESTION NUMBER:"
   print q
