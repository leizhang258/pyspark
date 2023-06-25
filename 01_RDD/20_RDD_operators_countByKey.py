# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("countByKey").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("hdfs://hadoop202:8020/data/input/words.txt")
    rdd2 = rdd.flatMap(lambda x : x.split(" ")).map(lambda x: (x,1))

    # 通过countByKey来对进行计数
    # Action 算子的返回值不是RDD，所以不能使用collect进行结果的收集
    result = rdd2.countByKey()

    print(result)
    print(type(result))