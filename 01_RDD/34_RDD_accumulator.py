# coding:utf8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("accumulator").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
    # count = 0
    # 使用spark提供的累加器变量
    acmlt = sc.accumulator(0)

    def map_func(data):
        #global count
        global acmlt
        # count += 1
        acmlt += 1
        print(acmlt)

    rdd2 = rdd.map(map_func)
    rdd2.collect()
    print(f"累加器 = {acmlt}")

    # 累加器如果被销毁后重新被使用，那么累加器会被重复计算
    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    print(f"累加器 = {acmlt}")