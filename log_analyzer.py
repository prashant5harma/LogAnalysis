import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.functions import *
from collections import Counter

q = sys.argv[2]
fr_hosts = sys.argv[3:]

conf = SparkConf().setAppName("ass").setMaster("local")
sc = SparkContext(conf=conf)

if q == str(1):
   print "* Q1: line counts"
   for word in fr_hosts:
   		input_text_file = word
   		output_text_file = "yalla"
   		print input_text_file
   		counts=sc.textFile(input_text_file).

   		counts=sc.textFile(input_text_file).flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
		counts.saveAsTextFile(output_text_file)
		#print counts
		#res = "  +  " + word + ":  " + str(counts)
		#print "COUNTS :  "
   
  

elif q == str(2):
   print "* Q2: sessions of user achille"
   print q

elif q == str(3):
   print "* Q3: unique user names"
   print q

elif q == str(4):
   print "* Q4: sessions per user"
   print q

elif q == str(5):
   print "* Q5: number of errors"
   print q

elif q == str(6):
   print "* Q6: 5 most frequent error messages"
   print q

elif q == str(7):
   print "* Q7: users who started a session on both hosts, i.e., on exactly 2 hosts."
   print q

elif q == str(8):
   print "* Q8: users who started a session on exactly one host, with host name."
   print q

elif q == str(9):
   print "Question number 9"
   print q

else:
   print "INVALID QUESTION NUMBER:"
   print q