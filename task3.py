import sys
import time
from operator import add
from pyspark import SparkContext
import math

def genCandidates(freq, l):
    #tuple(sorted(set(i) | set(j)))): 0 for i in frequent_itemset for j in frequent_itemset if len(set(i) | set(j)) == k
    #temp = {}
    if type(freq[0]) != frozenset:
        if isinstance(freq[0], str):
            freq = {frozenset([i]) for i in freq}
        else:
            freq = {frozenset(i) for i in freq}
    t = list({i.union(j) for i in freq for j in freq if len(i.union(j)) == l})
    '''
    #print(len(t))
    for i in t:
        temp[i] = 0
    for cand in temp.keys():
        for item in data:
            if cand.issubset(item):
                temp[cand] += 1
    '''
    return t

def countCandidates(lst, data, supp):
    temp = {}
    flag = 0
    for i in lst:
        temp[i] = 0

    for d in data:
        #print(d)
        set1 = set(d)
        for key in temp.keys():
            if isinstance(key, frozenset):
                for i in key:
                    if i in set1:
                        flag = 1
                    else:
                        break
                if flag == 1:
                    temp[key] += 1
            else:
                if key in set1:
                    temp[key] += 1
    res = []
    for key, v in temp.items():
        if v >= supp:
            res.append(key)
    #print(res)
    return res


def MRP1(bskt):
    data_chunk = list(bskt)
    ps = math.ceil(float(support)/(partitions/len(data_chunk)))
    candict = {}
    freq_item = []
    freq_items = []

    for chunk in data_chunk:
        for item in chunk:
            if item not in candict.keys():
                candict[item] = 1
            else:
                candict[item] += 1
    for key, v in candict.items():
        if v >= ps:
            freq_item.append(key)
    freq_items.append((sorted(freq_item), 1))
    k = 2
    itemset = [([i]) for i in freq_item]
    #newCandidates = {i: 1 for i in frequent_itemset}
    while len(itemset) > 0:
        frequent_itemset = genCandidates(itemset, k)
        itemset = countCandidates(frequent_itemset, data_chunk, ps)
        #print(itemset)
        for i in itemset:
            freq_items.append((i, k))

        if len(itemset) > 0:
            k += 1
        else:
            break
    return freq_items

def MRP2(iterator, candidates):
    bask = list(iterator)
    freqItemset = {}

    for b in bask:
        for candidate in candidates:
            for item in candidate[1]:
                if type(item) == str:
                    item = frozenset([item])
                if item in b or all([i in b for i in item]):
                    if item not in freqItemset.keys():
                        freqItemset[item] = 0
                    freqItemset[item] += 1

    for k in freqItemset.keys():
        if len(k) > 1:
            item_set = k
        else:
            item_set = tuple(k)
        yield item_set, freqItemset[k]

if __name__=="__main__":
    startTime = time.time()
    if len(sys.argv) !=5:
        print("error")
        sys.exit(-1)

    sc = SparkContext("local[*]")
    threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    output_file = sys.argv[4]

    input_file = sc.textFile(sys.argv[3]).map(lambda x: x.split(",")).filter(lambda x: x[0] != "user_id").map(lambda x: (x[0], x[1]))
    baskets = input_file.combineByKey(lambda x: [x], lambda i, n: append_list(i, n), lambda i, n: extend_list(i, n)).filter(lambda x: len(x) > threshold).map(lambda x: set(x[1])).persist()
    #print(baskets.take(2))

    partitions = baskets.count()

    Candidates = baskets.mapPartitions(MRP1).collect()
    print("here",len(Candidates))

    with open(output_file, "w") as file:
        file.write("Candidates: \n")
        for candidate in sorted(Candidates):
            if candidate[0] == 1:
                file.write(",".join(["('{}')".format(i) for i in candidate[1]]) + "\n")
            else:
                file.write(",".join(['{}'.format(tuple(i)) for i in candidate[1]]) + "\n")
            file.write("\n")
        file.write("\n")
    '''
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
    '''