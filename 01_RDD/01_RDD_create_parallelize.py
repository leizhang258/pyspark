# coding:utf8

# 导入spark相关包
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 初始化执行Spark环境
    conf = SparkConf().setAppName("create rdd").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # sc对象的parallelize方法，可以将本地集合转换成RDD返回给你
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])
    print("默认分区数：", rdd.getNumPartitions()) # 默认分区数： 8 （是因为该虚拟机hadoop202共有8个CPU Core）

    rdd = sc.parallelize([1,2,3], numSlices=3) # 这里指定分区数量
    print("默认分区数：", rdd.getNumPartitions())

    # 这里collect是action算子，将RDD中每个分区的数量，都发送到Driver中，形成一个python list对象
    # collect：分布式转 --> 本地集合
    print("RDD的内容：", rdd.collect())