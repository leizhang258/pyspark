# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # rdd = sc.parallelize(["hadoop spark flink", "spark hadoop hadoop", "hadoop flink"])
    rdd = sc.textFile("hdfs://hadoop202:8020/data/input/words.txt")

    rdd2 = rdd.map(lambda line:line.split(" "))
    print("=====================================Map============================================")
    print(rdd2.collect())

    print("===================================flatMap==========================================")
    rdd3 = rdd.flatMap(lambda line:line.split(" "))
    print(rdd3.collect())
