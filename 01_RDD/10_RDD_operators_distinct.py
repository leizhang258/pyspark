# coding:utf8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("yarn")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,1,2,3,4,3,4,5,9])
    print(rdd.distinct().collect())