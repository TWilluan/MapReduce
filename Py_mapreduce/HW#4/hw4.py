# Tuan Vo
# CPSC 4330
# HW_4

from pyspark import SparkContext
import sys
import re

K = 5
CONVERGE_DIST = 0.1

RAND_LOC = 5
SEED = 34
MAX_INT = 9999999999999999

def find_dis(random_cp, pos):
    index = index_min = 0
    min = MAX_INT

    for cp in random_cp:
        cp = pow(cp[0]-pos[0],2) + pow(cp[1]-pos[1],2)
        if (cp < min):
            min = cp
            index_min = index
        index += 1
    return ((random_cp[index_min][0], random_cp[index_min][1]), (pos[0], pos[1]))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: hw4.py <input> <output>"
        exit(-1)

    sc = SparkContext()
    file = sc.textFile(sys.argv[1])

    pos = file.map(lambda line : line.split(",")) \
            .map(lambda field : (float(field[3]), float(field[4])) ) \
            .filter(lambda field : (field[0] != 0) and (field[1] != 0)) \
            .persist()
    
    random_cp = pos.takeSample(False, RAND_LOC, SEED)


    run = True
    while(run):
        temp_dist = 0
        close = pos.map(lambda field : find_dis(random_cp, field)).groupByKey()

        new = close.map(lambda fields: ((fields[0][0], fields[0][1]),      \
                ((sum(field[0] for field in fields[1])/len(fields[1])),    \
                (sum(field[1] for field in fields[1])/len(fields[1])))))

        for cp in new.collect():
            curr_cp = cp[0]
            new_cp = cp[1]
            temp_dist += pow(curr_cp[0] - new_cp[0], 2) + pow(curr_cp[1] - new_cp[1], 2)

        random_cp = []
        for cp in new.collect():
            new = cp[1]
            random_cp.append(new)

        if (temp_dist <= CONVERGE_DIST):
            run = False
    
    for cp in random_cp:
        print(cp)
    

