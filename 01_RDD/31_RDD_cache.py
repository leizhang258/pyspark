# coding:utf8
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
import time

if __name__ == '__main__':
    conf = SparkConf().setAppName("cache").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile("hdfs://hadoop202:8020/data/input/words.txt")
    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x ,1))

    ## 这里加上缓存
    rdd3.cache()
    rdd3.persist(StorageLevel.MEMORY_AND_DISK_2)

    #这里执行完rdd4，rdd3就会被销毁
    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    start_time = time.time()
    # rdd3被再次使用时，原来的已经被销毁，spark会重新根据之前的逻辑计算rdd3
    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())
    print(f"Run Time: {time.time() - start_time}") # 0.25998640060424805

    # 释放缓存
    rdd3.unpersist()

    time.sleep(1000)

