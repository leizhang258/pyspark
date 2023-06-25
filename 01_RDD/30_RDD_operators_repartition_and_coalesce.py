# coding:utf8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("repartition_and_coalesce").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,54,5,6,7,8],3)

    # repartition 修改分区
    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())

    # coalesce 修改分区，增加分区时需要加上shuffle=True参数，否则修改不成功
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())