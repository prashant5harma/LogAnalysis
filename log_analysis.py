from pyspark import SparkConf, SparkContext

text_file = sc.textFile("/Users/prashantsharma/Documents/LogAnalysis/input.txt")
#counts = text_file.flatMap(lambda line: line.split(" ")) \
#             .map(lambda word: (word, 1)) \
#             .reduceByKey(lambda a, b: a + b)
#counts.saveAsTextFile("/Users/prashantsharma/Documents/LogAnalysis/output.txt")


# Creates a DataFrame having a single column named "line"
df = text_file.map(lambda r: Row(r)).toDF(["line"])
errors = df.filter(col("line").like("errors"))
# Counts all the errors
print(errors.count())

# Counts errors mentioning MySQL
#errors.filter(col("line").like("%MySQL%")).count()
# Fetches the MySQL errors as an array of strings
#errors.filter(col("line").like("%MySQL%")).collect()
