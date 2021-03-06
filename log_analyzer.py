import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.functions import *
from collections import Counter
from pyspark.sql import SparkSession
q = sys.argv[2]
fr_hosts = sys.argv[3:]
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
       log = [wrd[:-1] for wrd in log]
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
    v1 = sys.argv[3]
    v2 = sys.argv[4]
    print "* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts."
    count1=sc.textFile(v1).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1])
    count2=sc.textFile(v2).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1])
    cnt = count1.intersection(count2).collect()
    print " + : " + str(cnt)

elif q == str(8):
    v1 = sys.argv[3]
    v2 = sys.argv[4]
    print "* Q8: users who started a session on exactly one host, with host name."
    con1=sc.textFile(v1).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct()
    con2=sc.textFile(v2).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct()
    cona = con1.collect()
    conb = con2.collect()
    intersection = con1.intersection(con2)
    union = con1.union(con2)
    cnt = union.subtract(intersection).distinct().collect()
    mylist = [["try","try"]]
    for val in cnt:
        if val in cona:
            mylist.append([val,v1])
        elif val in conb:
            mylist.append([val,v2])
    mylist.remove(["try","try"])
    print " + : " + str(mylist)



elif q == str(9):
    v1 = sys.argv[3]
    v2 = sys.argv[4]
    spark = SparkSession(sc)
    counts=sc.textFile(v1).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct()
    arr = counts.collect()
    # to remove the full stop
    arr = [word[:-1] for word in arr]
    #make a df of the text file
    text_file = sc.textFile(v1)
    df = text_file.map(lambda r: Row(r)).toDF(["line"])
    #Loop to find all words in arr and replace them
    dlist = [["try","try"]]
    for word in arr:
        a = word
        b = "User-" + str(arr.index(word))
        dlist.append([a,b])
        df.replace(a,b)
    dlist.remove(["try","try"])
    print " + : " + str(dlist)
    #write df to file
    import os
    cwd = os.getcwd()
    fn = cwd + "/Anonymized_logs"
    df.write.format("text").save(fn)

else:
   print "INVALID QUESTION NUMBER:"
   print q





   #counts=sc.textFile(v1).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct()
   #arr = counts.zipWithIndex().collect()
   #reff = sc.parallelize(arr).map(lambda x:(x[0],"user-"+str(x[1])))
   #reffDef = reff.map(lambda r: Row(r)).toDF(["users"]).show()
   #counts2=sc.textFile(v2).filter(lambda line: "Starting Session" in line).map(lambda x: (1, x.split()[::-1])).map(lambda x:(x[0],x[1][0])).map(lambda x:x[1]).distinct()
   #arr2 = counts2.zipWithIndex().collect()
   #reff2 = sc.parallelize(arr2).map(lambda x:(x[0],"user-"+str(x[1])))
   #reffDef2 = reff2.map(lambda r: Row(r)).toDF(["users"]).show()
