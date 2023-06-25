# coding:utf8

from pyspark import SparkConf, SparkContext
import os

# 在windows上运行需要临时指定pyspark python的环境变量
# os.environ['PYSPARK_PYTHON'] = "C:\\Users\\Leizh\\.conda\\envs\\pyspark\\python.exe"

if __name__ == '__main__':

    # 通过spark-submit提交代码时就不需要指定setMaster了
    # conf = SparkConf().setMaster("local[*]").setAppName("WordCountHelloWorld")
    conf = SparkConf().setAppName("WordCountHelloWorld")

    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求 ： Wordcount单词数，读取HDFS上的words.txt文件，对其内部的单词统计出现的数量
    # 读取文件
    # 读取本地文件
    #file_rdd = sc.textFile("../data/input/words.txt")

    # 读取HDFS文件
    file_rdd = sc.textFile("hdfs://hadoop202:8020/test.txt")

    # 将单词进行切割，得到一个存储全部单词的集合对象
    word_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # 将单词转换为元组对象，key是单词，value是数字1
    words_with_one_rdd = word_rdd.map(lambda x: (x, 1))

    # 将元组的value按照key进行分组，对所有的value执行聚合操作(相加)
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect方法收集RDD的数据打印输出结果
    print(result_rdd.collect())