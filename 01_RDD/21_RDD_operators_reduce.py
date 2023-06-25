# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("reducet").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6])
    print(rdd.reduce(lambda a, b : a + b))