# coding:utf8
from pyspark import SparkConf, SparkContext
import time
if __name__ == '__main__':
    conf = SparkConf().setAppName("checkpoint").setMaster("yarn")
    sc = SparkContext(conf=conf)

    # 1. 告知spark，开启CheckPoint功能
    sc.setCheckpointDir("hdfs://hadoop202:8020/data/output/checkpoint")

    rdd1 = sc.textFile("hdfs://hadoop202:8020/data/input/words.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x ,1))

    # 调用checkPoint API保存数据到hdfs
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())