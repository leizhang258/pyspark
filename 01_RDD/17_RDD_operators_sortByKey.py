# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('C', 1), ('B', 1), ('a', 1), ('M', 2), ('o', 1), ('b', 1), ('y', 1), ('Z', 2)])

    sort_by_key_rdd = rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower())

    print(sort_by_key_rdd.collect())