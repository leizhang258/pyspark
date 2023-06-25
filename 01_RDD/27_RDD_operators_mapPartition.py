# coding:utf8
from pyspark import SparkConf, SparkContext

def process(iter):
    result = list()
    for item in iter:
        result.append(item * 10)
    return result

if __name__ == '__main__':
    conf = SparkConf().setAppName("mapPartitions").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,67,'hello','cool',3.12],3)

    print(rdd.mapPartitions(process).collect())