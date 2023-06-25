# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1. 读取文件获取数据构建RDD
    file_rdd = sc.textFile("hdfs://hadoop202:8020/data/input/words.txt")

    print("======================2.通过flatMap取出所有的单词======================")
    word_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    print(word_rdd.collect())

    # 3. 将单词转换成元组，key是单词，value是1
    print("===================3. 将单词转换成元组，key是单词，value是1==================")
    word_with_on_rdd = word_rdd.map(lambda word: (word, 1))
    print(word_with_on_rdd.collect())

    print("===================4. 用reduceByKey，对单词进行分组并进行value的聚合==================")
    # 4. 用reduceByKey，对单词进行分组并进行value的聚合
    result_rdd = word_with_on_rdd.reduceByKey(lambda a, b: a + b)
    # 5. 通过collect算子，将rdd的数据收集到driver中，打印输出
    print(result_rdd.collect())