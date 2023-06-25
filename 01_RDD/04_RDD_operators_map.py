# coding:utf8

from pyspark import SparkConf, SparkContext

# 定义方法，作为算子的传入函数体
def add(data):
    return data * 10

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    print(rdd.map(add).collect())

    # 更简单的方式，定义lambda匿名函数
    print(rdd.map(lambda data: data * 10).collect())