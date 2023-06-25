# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("takeSample").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,1,2,2,3,4,5,6,7,8,9,10,11])

    print(rdd.takeSample(False, 5, 2))