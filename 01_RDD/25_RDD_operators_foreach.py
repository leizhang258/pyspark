# coding:utf8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("foreach").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6,7,8],1)
    result = rdd.foreach(lambda x: print(x * 10))