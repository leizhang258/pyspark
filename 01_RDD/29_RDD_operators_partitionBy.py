# coding:utf8
from pyspark import SparkConf, SparkContext

def process(k):
    if 'hadoop' == k: return 0
    if 'spark' == k: return 1
    return 2

if __name__ == '__main__':
    conf = SparkConf().setAppName("partitionBy").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop',1),('spark',1),('flink',1),('hadoop',1),('flink',1),('spark',1),('spark',1),])
    print(rdd.partitionBy(3,process).glom().collect())