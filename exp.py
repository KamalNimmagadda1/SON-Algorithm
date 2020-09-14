import sys
import time
from pyspark import SparkContext
import math

def MRP1(bskt):
    data_chunk = list(bskt)
    ps = math.ceil(float(support)/(partitions/len(data_chunk)))
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
    frequent_itemset = [frozenset([i]) for i in freq_item]
    newCandidates = list({i.union(j) for i in frequent_itemset for j in frequent_itemset if len(i.union(j)) == k})
    #print(newCandidates, "test")
    if len(newCandidates):
        n = {}
        for each in newCandidates:
            n[each] = 0
        for cand in n.keys():
            for item in data_chunk:
                flag = 0
                for i in cand:
                    if i in item:
                        flag = 1
                    else:
                        flag = 0
                if flag == 1:
                    n[cand] += 1
        #newCandidates = n
        #print("new candies", n)
        frequent_itemset = []
        for key, v in n.items():
            if v >= ps:
                frequent_itemset.append(key)
        print("freqs", len(frequent_itemset))
        yield k, sorted(frequent_itemset)
        if len(frequent_itemset) > 0:
            k += 1
            newCandidates = list({i.union(j) for i in frequent_itemset for j in frequent_itemset if len(i.union(j)) == k})

        # else:
        #     break


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
    threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    output_file = sys.argv[4]
    input_file = sc.textFile(sys.argv[3]).map(lambda x: x.split(",")).filter(lambda x: x[0] != "user_id").map(lambda x: (str(x[0]), str(x[1])))

    baskets = input_file.groupByKey().filter(lambda x: len(x[1]) > threshold).map(lambda x: list(set(x[1]))).persist()
    #print(baskets.take(5))
    partitions = baskets.count()

    Candidates = baskets.mapPartitions(MRP1).reduceByKey(lambda a, b: sorted(set(a+b))).collect()

    # #print(Candidates)
    #
    # with open(output_file, "w") as file:
    #     file.write("Candidates: \n")
    #     for candidate in sorted(Candidates):
    #         if candidate[0] == 1:
    #             file.write(",".join(["('{}')".format(i) for i in candidate[1]]) + "\n")
    #         else:
    #             file.write(",".join(['{}'.format(i) for i in candidate[1]]) + "\n")
    #         file.write("\n")
    #     file.write("\n")

    # Frequent_Itemsets = baskets.mapPartitions(lambda x: MRP2(x, Candidates)).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support).collect()
    # result = {}
    # for itemset in Frequent_Itemsets:
    #     if type(itemset[0]) == tuple:
    #         if len(itemset[0]) not in result.keys():
    #             result[len(itemset[0])] = []
    #         result[len(itemset[0])].append(itemset[0])
    #         result[len(itemset[0])] = sorted(result[len(itemset[0])])
    #     else:
    #         if 1 not in result.keys():
    #             result[1] = []
    #         result[1].append(itemset[0])
    #         result[1] = sorted(result[1])
    #
    # #print(result.items())
    #
    # with open(output_file, "a") as file:
    #     file.write("Frequent Itemsets: \n")
    #     for k in sorted(result.keys()):
    #         if k == 1:
    #             file.write(",".join(["('{}')".format(i) for i in result[k]]) + "\n")
    #         else:
    #             file.write(",".join(['{}'.format(i) for i in result[k]]) + "\n")
    #         file.write("\n")
    # TotalTime = time.time() - startTime
    #
    # print(f"Duration: {TotalTime}")
