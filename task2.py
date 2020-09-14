import sys
import csv
import time
from operator import add
import itertools
from collections import OrderedDict
from pyspark import SparkContext
import math


def genCandidates(freq, l):
    print("marker3")

    Set1 = set()
    if l == 1:
        for i in freq:
            Set1 = i[1] | Set1
        Set1 = list(Set1)

    else:
        if isinstance(freq[0], str):
            print("marker4")
            freq = {frozenset([i]) for i in freq}
        Set1 = list({i.union(j) for i in freq for j in freq if len(i.union(j)) == l})
    return Set1


def countCandidates(candidates, transactions, support):
    print("marker5")
    d = {}
    fl = 0

    for x in candidates:
        d[x] = 0

    for transaction in transactions:

        item_set= set(transaction[1])
        for k in d:
            if isinstance(k,frozenset):
                for x in k:
                    if x in item_set:
                        fl = 1
                    else:
                        fl = 0
                        break
                if fl==1:
                    d[k]+=1

            else:
                if k in item_set:
                    d[k]+=1
    ret = []
    print("marker6")
    for k in d:
        if d[k]>=support:
            ret.append(k)
    return ret




def MRP1(bskt, support, partitions):
    print("marker1")
    data_chunk = list(bskt)
    ps = (support*len(data_chunk))/partitions
    candict = {}
    freq_item = []
    freq_items = []

    #freq_items.append((sorted(freq_item), 1))
    k = 1
    candidates = genCandidates(data_chunk, k)
    #newCandidates = {i: 1 for i in frequent_itemset}
    while len(candidates) > 0:
        f = 0
        if f == 0:
            print("marker2")
            flag = 1
        k += 1

        itemset = countCandidates(candidates, data_chunk, ps)
        for i in itemset:
            freq_items.append((i, k-1))
        #print(len(itemset))
        if len(itemset) > 0:
            candidates = genCandidates(list(itemset), k)
        else:
            break
    return freq_items

def mergeVal(i, n):
    i.append(n)
    return i

def mergeComb(i, n):
    i.extend(n)
    return i

if __name__=="__main__":
    startTime = time.time()
    if len(sys.argv) !=5:
        print("error")
        sys.exit(-1)

    sc = SparkContext("local[*]")
    threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    output_file = sys.argv[4]

    input_file = sc.textFile(sys.argv[3]).filter(lambda x: x != "user_id,business_id").map(lambda x: x.split(",")).map(lambda x: (x[0], x[1]))
    baskets = input_file.combineByKey(lambda x: [x], lambda i, n: mergeVal(i, n), lambda i, n: mergeComb(i, n)).filter(lambda x: len(x[1])>threshold).map(lambda x: (x[0], set(x[1]))).persist()
    # print(baskets.count())
    partitions = baskets.count()
    Candidates = baskets.mapPartitions(lambda x: MRP1(x, support, partitions)).collect()
    print("Duration", time.time()-startTime)
    # print("here",len(Candidates))
    # candict = {}
    # tlist = []
    # l = 0
    #
    # while l < len(Candidates):
    #     if Candidates[l][1] in candict:
    #         if Candidates[l][1] == 1:
    #             candict[Candidates[l][1]].append(list([Candidates[l][0]]))
    #             l += 1
    #             continue
    #         candict[Candidates[l][1]].append(list([i for i in Candidates[l][0]]))
    #     else:
    #         if Candidates[l][1] == 1:
    #             candict[Candidates[l][1]] = list([[Candidates[l][0]]])
    #             l += 1
    #             continue
    #         candict[Candidates[l][1]] = list([[i for i in Candidates[l][0]]])
    #     l += 1
    #
    # fwriter = "Candidates: \n"
    # for d in candict:
    #     candict[d] = [sorted(list(x) for x in candict[d])]
    #     candict[d] = sorted(candict[d])
    #     for x in candict[d]:
    #         if len(tuple(x)) == 1:
    #             fwriter += str(tuple(x))[0:len(str(tuple(x)))-2] + "),"
    #         else:
    #             fwriter += str(tuple(x)) + ","
    #     fwriter = fwriter[0:len(fwriter)-1]
    #     fwriter += "\n\n"
    # fwriter = fwriter[0:len(fwriter)-1]
    #
    # with open(output_file, "w") as file:
    #     file.write(fwriter + "\n")
