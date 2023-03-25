# Name: Tuan Vo
# CPSC 4330
# Assignment_3 -> problem_1

from pyspark import SparkContext
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: WordCount.py <input>  <output>"
        exit(-1)    
    sc = SparkContext()
    textfile = sc.textFile(sys.argv[1])
    header = textfile.first()
    textfile = textfile.filter(lambda line : line != header) #extract header

    res = textfile.map(lambda line : line.split("\t")) \
                    .map(lambda field : (field[3], (1, int(field[7])))) \
                    .reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])) \
                    .sortByKey(ascending=True) \
                    .map(lambda out : (out[0], float(out[1][1]/out[1][0]), out[1][0]))
    
    res.saveAsTextFile(sys.argv[2])
    sc.stop()