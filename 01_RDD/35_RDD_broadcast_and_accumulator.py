# coding:utf8
from pyspark import SparkConf, SparkContext
import re

if __name__ == '__main__':
    conf = SparkConf().setAppName("broadcast and accumulator").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    abnormal_char = [",", ".", "!", "#", "$", "%"]
    broadcast = sc.broadcast(abnormal_char)
    accumulator = sc.accumulator(0)

    rdd = sc.textFile("hdfs://hadoop202:8020/data/input/accumulator_broadcast_data.txt")
    rdd.cache()

    # 通过filter去除数据空行，在python中有内容就是True, None就是False
    # 通过map去除每行行首和行末的空格
    # 正则表达式 \s+ 表示，不确定多少空格，最少一个空格
    rdd1 = rdd.filter(lambda line:line.strip()).map(lambda line: line.strip()).flatMap(lambda line: re.split("\s+", line))

    def abnormal_char_filter(data):
        global accumulator
        if data in broadcast.value:
            accumulator += 1
            return False
        else:
            return True
        print(accumulator)
    rdd2 = rdd1.filter(abnormal_char_filter)

    rdd3 = rdd2.map(lambda x:(x,1)).reduceByKey(lambda a, b: a + b)
    print(rdd3.collect())
    print(f"累加器 = {accumulator}")
