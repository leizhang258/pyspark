# coding:utf8
from pyspark import SparkConf, SparkContext
import json
from defs_19 import city_with_category
import os

# Yarn集群模式可能要指定 HADOOP_CONF_DIR 的路径
os.environ['HADOOP_CONF_DIR'] = "/opt/module/hadoop-3.3.1/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("yarn_test_01").setMaster("yarn")

    #如果提交到集群运行，除了主代码以外，还需要依赖其他代码
    #需要设置一个参数，来告知Pyspark，还有依赖文件要同步上传至其他集群
    # 参数叫做：spark.submit.pyFiles
    # 参数的值可以是单个 .py文件，也可以是 .zip压缩包(有多个依赖文件的时候可以用zip压缩后上传)
    conf.set("spark.submit.pyFiles", "defs_19.py")

    sc = SparkContext(conf=conf)

    print("=================================================读取数据文件=================================================")
    rdd = sc.textFile("hdfs://hadoop202:8020/data/input/order.text")
    print(rdd.collect())

    # 通过rdd数据的split按照|符号进行切分，得到一个个json的数据
    print("==============================通过rdd数据的split按照|符号进行切分，得到一个个json的数据==============================")
    json_rdd = rdd.flatMap(lambda line: line.split("|"))
    print(json_rdd.collect())

    # 通过python内置的json库，完成json字符串到字典的转换
    print("=================================通过python内置的json库，完成json字符串到字典的转换=================================")
    dict_rdd = json_rdd.map(lambda json_str: json.loads(json_str))
    print(dict_rdd.collect())

    #过滤数据只保留北京数据
    print("==============================================过滤数据只保留北京数据=============================================")
    beijing_rdd = dict_rdd.filter(lambda d:d['areaName']=="北京")
    print(beijing_rdd.collect())

    #组合北京和商品类型组成新的字符串
    print("==========================================组合北京和商品类型组成新的字符串==========================================")
    category_rdd = beijing_rdd.map(city_with_category)
    print(category_rdd.collect())

    # 对结果集进行去重
    print("================================================对结果集进行去重================================================")
    result_rdd = category_rdd.distinct()

    print(result_rdd.collect())