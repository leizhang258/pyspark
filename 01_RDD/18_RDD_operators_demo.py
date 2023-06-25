# coding:utf8
from pyspark import SparkConf, SparkContext
import json

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("yarn")
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
    category_rdd = beijing_rdd.map(lambda x:x["areaName"] + "_" + x["category"])
    print(category_rdd.collect())

    # 对结果集进行去重
    print("================================================对结果集进行去重================================================")
    result_rdd = category_rdd.distinct()

    print(result_rdd.collect())