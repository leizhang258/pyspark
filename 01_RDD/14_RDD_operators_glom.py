# coding:utf8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)
    print(rdd.glom().collect())
    print(rdd.glom().flatMap(lambda x:x).collect())