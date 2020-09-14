import sys
import json
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local[*]")
    output_file = sys.argv[4]
    state_filter = str(sys.argv[3])
    review_file = sc.textFile(sys.argv[1]).map(lambda x: json.loads(x)).map(lambda x: (x["business_id"], x["user_id"]))
    business_file = sc.textFile(sys.argv[2]).map(lambda x: json.loads(x)).filter(lambda x: x["state"] == state_filter).map(lambda x: (x["business_id"], 1))

    BusinessRDD = business_file.join(review_file).map(lambda x: (x[1][1], x[0])).collect()
    #print(BusinessRDD.take(3))
    with open(output_file, "w") as f:
        f.write("user_id,business_id\n")
        for i in BusinessRDD:
            f.write('{0},{1}\n'.format(i[0], i[1]))
