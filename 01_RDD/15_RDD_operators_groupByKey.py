# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1), ('a',1), ('b',1), ('b', 2)])
    group_rdd = rdd.groupByKey()
    print(group_rdd.map(lambda x: (x[0], list(x[1]))).collect())