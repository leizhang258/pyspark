# coding:utf8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1),('a', 1), ('b', 1), ('a', 1),('a', 1), ('b', 1)])

    # 通过groupBy对数据进行分组
    # groupBy传入的函数的意思是：通过这个函数，确定按照谁来进行分组（返回谁即可）
    # 分组规则和SQL是一致的，也就是相同的在一个组(hash分组)
    result = rdd.groupBy(lambda t: t[0])
    print(result.collect())

    print(result.map(lambda t: (t[0], list(t[1]))).collect())