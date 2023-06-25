# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 通过textFile API 读取该数据

    # 读取本地文件数据
    file_rdd1 = sc.textFile("../data/input/words.txt") # 默认读取分区数 2, 文件读取方式创建RDD时，分区数仅和文件大小有关，和CPU无关
    print("默认读取分区数", file_rdd1.getNumPartitions())
    print("file_rdd 内容", file_rdd1)

    # 加最小分区数的测试
    file_rdd2 = sc.textFile("../data/input/words.txt", 3)  # 默认读取分区数 3

    # 最小分区数是参考值，Spark有自己的判断，如果分区数给太大，Spark将不予理会
    file_rdd3 = sc.textFile("../data/input/words.txt", 100)  # 默认读取分区数 51

    print("默认读取分区数", file_rdd2.getNumPartitions())
    print("默认读取分区数", file_rdd3.getNumPartitions())

    # 读取HDFS文件测试
    hdfs_rdd = sc.textFile("hdfs://hadoop202:8020/test.txt")
    print("默认读取分区数", hdfs_rdd.getNumPartitions())  # 默认读取分区数 2