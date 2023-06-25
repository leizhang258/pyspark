# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(1001, "kevin"), (1002, "mark"), (1003, "jill"), (1004, "david")])
    rdd2 = sc.parallelize([(1001, "IT"), (1002, "Marketing")])

    print("=============================inner join=============================")
    print(rdd1.join(rdd2).collect())

    print("===========================left outer join===========================")
    print(rdd1.leftOuterJoin(rdd2).collect())