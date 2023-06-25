# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("df_create_6").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("json").\
        load("hdfs://hadoop202:8020/data/input/sql/people.json")
    df.printSchema()
    df.show()