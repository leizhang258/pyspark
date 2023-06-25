# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("data cleansing").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext
    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", True).\
        load("hdfs://hadoop202:8020/data/input/sql/people.csv")
    df.cache()

    # 数据清洗，数据去重
    # dropDuplicates 是DataFrame的API，可以完成数据去重
    # 无参数使用，对全部的列。联合起来进行比较，去去除重复项，只保留一条
    # 也可以传入包含column name的list以某些列进行去重
    df.dropDuplicates().show()
    df.dropDuplicates(['age','job']).show()

    # 数据清洗：缺失值处理
    # dropna API是可以对缺失值进行删除
    # 无参数使用，只要 行里面有null，则会直接删除这一行
    df.dropna().show()
    # 最少满足三个有效列，否则会被删除
    df.dropna(thresh=3).show()
    df.dropna(thresh=2, subset=['name','age']).show()

    # 数据清洗，缺失值填充
    # DataFrame 的 fillna，对缺失值进行填充
    df.fillna("loss").show()
    # 指定需要填充的列
    df.fillna("N/A", subset=['job']).show()
    # 定义一个字典，对里面所有的列提供填充规则
    df.fillna({"name":"未知姓名", "age": 1, "job": "worker"}).show()

