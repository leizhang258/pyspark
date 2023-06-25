# coding:utf8
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName("saveAsTextFile").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6,7,8,"hello",3.22],3)

    rdd.saveAsTextFile("hdfs://hadoop202:8020/data/output/testout2")