# Name: Tuan Vo
# CPSC 4330
# Assignment_3 -> problem_2

from pyspark import SparkContext
import sys

def filter_(word):
    list = open("stopwords.txt").read().splitlines()
    if word not in list and word != '':
        return word

def clean_str(x):
  punc='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~'
  str = x.lower()
  for ch in punc:
    str = str.replace(ch, '')
  return str

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: WordCount.py <input>  <output>"
        exit(-1)    
    sc = SparkContext()
    textfile = sc.textFile(sys.argv[1])

    #clean data
    clean = textfile.flatMap(lambda line : line.split()) \
                    .map(clean_str) \
                    .map(filter_) \
                    .filter(lambda x : x is not None)
    
    #output the result
    res = clean.map(lambda field : (field, 1)) \
                .reduceByKey(lambda x, y : x+y) \
                .sortBy(lambda x : -x[1])

    #print output
    for i in res.take(10):
        print(i[0])
    sc.stop()