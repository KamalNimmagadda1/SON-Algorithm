import sys
import time
from pyspark import SparkContext
import math

def MRP1(bskt):
    data_chunk = list(bskt)
    #print(data_chunk[0])
    ps = math.ceil(float(support)/partitions)
    candict = {}
    freq_item = []

    for chunk in data_chunk:
        for item in chunk:
            if item not in candict.keys():
                candict[item] = 1
            else:
                candict[item] += 1
    for k, v in candict.items():
        if v >= ps:
            freq_item.append(k)
    yield 1, sorted(freq_item)

    k = 2
    frequent_itemset = [([i]) for i in freq_item]
    while True:
        newCandidates = {tuple(sorted(set(i) | set(j))): 0 for i in frequent_itemset for j in frequent_itemset if len(set(i) | set(j)) == k}
        if len(newCandidates) > 0:
            for cand in newCandidates.keys():
                for item in data_chunk:
                    if all(1 if i in item else 0 for i in cand):
                        newCandidates[cand] += 1
            #frequent_itemset = [k for k, v in newCandidates.items() if v >= ps]
            frequent_itemset = []
            for key, v in newCandidates.items():
                if v >= ps:
                    frequent_itemset.append(key)
            if len(frequent_itemset) > 0:
                yield k, sorted(frequent_itemset)
                k += 1
            else:
                break
        else:
            break

def MRP2(iterator, candidates):
    bask = list(iterator)
    freqItemset = {}

    for b in bask:
        for candidate in candidates:
            for item in candidate[1]:
                if item in b or all([i in b for i in item]):
                    if item not in freqItemset.keys():
                        freqItemset[item] = 0
                    freqItemset[item] += 1

    for k in freqItemset.keys():
        if len(k) > 1:
            item_set = k
        else:
            item_set = tuple([k])
        yield item_set, freqItemset[k]

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

    baskets = input_file.groupByKey().map(lambda x: list(set(x[1])))
    #print(baskets.take(5))
    partitions = baskets.getNumPartitions()

    Candidates = baskets.mapPartitions(MRP1).reduceByKey(lambda a, b: sorted(set(a+b))).collect()

    #print(Candidates)

    with open(output_file, "w") as file:
        file.write("Candidates: \n")
        for candidate in sorted(Candidates):
            if candidate[0] == 1:
                file.write(",".join(["('{}')".format(i) for i in candidate[1]]) + "\n")
            else:
                file.write(",".join(['{}'.format(i) for i in candidate[1]]) + "\n")
            file.write("\n")
        file.write("\n")

    Frequent_Itemsets = baskets.mapPartitions(lambda x: MRP2(x, Candidates)).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support).collect()
    result = {}
    for itemset in Frequent_Itemsets:
        if type(itemset[0]) == tuple:
            if len(itemset[0]) not in result.keys():
                result[len(itemset[0])] = []
            result[len(itemset[0])].append(itemset[0])
            result[len(itemset[0])] = sorted(result[len(itemset[0])])
        else:
            if 1 not in result.keys():
                result[1] = []
            result[1].append(itemset[0])
            result[1] = sorted(result[1])

    #print(result.items())

    with open(output_file, "a") as file:
        file.write("Frequent Itemsets: \n")
        for k in sorted(result.keys()):
            if k == 1:
                file.write(",".join(["('{}')".format(i) for i in result[k]]) + "\n")
            else:
                file.write(",".join(['{}'.format(i) for i in result[k]]) + "\n")
            file.write("\n")
    TotalTime = time.time() - startTime

    print(f"Duration: {TotalTime}")
