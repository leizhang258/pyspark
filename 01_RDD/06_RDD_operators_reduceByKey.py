# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1), ('a',1), ('b',1), ('b', 2)])

    #reduceByKey 对应的key的数据执行聚合相加
    print(rdd.reduceByKey(lambda a, b: a + b).collect())