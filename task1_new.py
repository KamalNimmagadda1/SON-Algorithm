import sys
import math
import time
from operator import add
from pyspark import SparkContext

def countCandidates(transactions, candidates, support):
    data = dict()
    flag = 0

    for x in candidates:
        data[x] = 0

    for transaction in transactions:

        item_set = set(transaction[1])
        for key in data:
            if type(key) == frozenset:
                for x in key:
                    if x in item_set:
                        flag = 1
                    else:
                        flag = 0
                        break
                if flag == 1:
                    data[key] += 1

            else:
                if key in item_set:
                    data[key] += 1
    sol = []
    for key in data:
        if data[key] >= support:
            sol.append(key)
    return sol


def MRP1(bskt, support, partitions):
    data_chunk = []
    for data in bskt:
        data_chunk.append(data)
    ps = float(support * len(data_chunk))/partitions
    candidates = set()
    freq_items = []
    for chunk in data_chunk:
        candidates = chunk[1] | candidates
    candidates = list(candidates)

    k = 1
    while len(candidates) > 0:
        k += 1

        itemset = countCandidates(data_chunk, candidates, ps)
        for i in itemset:
            freq_items.append((i, k-1))

        if len(itemset) > 0:
            if type(itemset[0]) == str:
                itemset = {frozenset([i]) for i in itemset}
            candidates = list({i.union(j) for i in itemset for j in itemset if len(i.union(j)) == k})
        else:
            break
    return freq_items

def MRP2(iterator, candidates):
    flag = 0
    freqItemset = dict()
    data_chunk = []

    for candidate in candidates:
        freqItemset[candidate[0]] = 0
    for i in iterator:
        data_chunk.append(i)

    for chunk in data_chunk:
        for items in candidates:
            if type(items[0]) == frozenset:
                for item in items[0]:
                    if item in chunk[1]:
                        flag = 1
                    else:
                        flag = 0
                        break
                if flag == 1:
                    freqItemset[items[0]] += 1
                    flag = 0
            else:
                if items[0] in chunk[1]:
                    freqItemset[items[0]] += 1

    lst = []
    for key, val in freqItemset.items():
        lst.append((key, val))
    return lst

def mergeVal(i, n, txt):
    if txt == "append":
        i.append(n)
    else:
        i.extend(n)
    return i

if __name__=="__main__":
    startTime = time.time()
    if len(sys.argv) !=5:
        print("error")
        sys.exit(-1)

    sc = SparkContext("local[*]")
    case = int(sys.argv[1])
    support = int(sys.argv[2])
    output_file = sys.argv[4]

    if case == 1:
        input_file = sc.textFile(sys.argv[3]).map(lambda x: x.split(",")).filter(lambda x: x[0] != "user_id").map(lambda x: (int(x[0]), str(x[1])))
    elif case == 2:
        input_file = sc.textFile(sys.argv[3]).map(lambda x: x.split(",")).filter(lambda x: x[0] != "user_id").map(lambda x: (int(x[1]), str(x[0])))

    baskets = input_file.combineByKey(lambda x: [x], lambda i, n: mergeVal(i, n, "append"), lambda i, n: mergeVal(i, n, "extend")).map(lambda x: (x[0], set(x[1]))).persist()

    partitions = baskets.count()
    Candidates = baskets.mapPartitions(lambda x: MRP1(x, support, partitions)).collect()
    candict = {}
    l = 0

    for candidate in Candidates:
        if type(candidate[0]) == frozenset:
            if candidate[1] not in candict.keys():
                candict[candidate[1]] = []
            candict[candidate[1]].append(candidate[0])
        else:
            if 1 not in candict.keys():
                candict[1] = []
            candict[1].append([candidate[0]])

    with open(output_file, "w") as f:
        f.write("Candidates: \n")
        for d in candict:
            fwriter = ""
            candict[d] = sorted(sorted(x) for x in candict[d])
            for x in candict[d]:
                if len(tuple(x)) == 1:
                    fwriter += str(tuple(x))[0:len(str(tuple(x)))-2] + "),"
                else:
                    fwriter += str(tuple(x)) + ","
            fwriter = fwriter[:len(fwriter)-1]
            f.write(fwriter + "\n\n")
        f.write("\n")

    Frequent_Itemsets = baskets.mapPartitions(lambda x: MRP2(x, Candidates)).reduceByKey(add).filter(lambda x: x[1] >= support).collect()
    result = {}

    for itemset in Frequent_Itemsets:
        if type(itemset[0]) == frozenset:
            if len(itemset[0]) not in result.keys():
                result[len(itemset[0])] = []
            result[len(itemset[0])].append(itemset[0])
        else:
            if 1 not in result.keys():
                result[1] = []
            result[1].append([itemset[0]])

    for lvl, vals in result.items():
        result[lvl] = sorted(tuple(sorted(x)) for x in result[lvl])

    with open(output_file, "a") as f:
        f.write("Frequent Itemsets: \n")
        for k in sorted(result.keys()):
            if k == 1:
                f.write(",".join(["{}".format(str(i)[:len(str(i))-2] + ")") for i in result[k]]) + "\n")
            else:
                f.write(",".join(["{}".format(i) for i in result[k]]) + "\n")
            f.write("\n")
    TotalTime = time.time() - startTime

    print("Duration: ", TotalTime)

