# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("fold").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1,10), 3)
    print(rdd.fold(10, lambda a, b: a + b))