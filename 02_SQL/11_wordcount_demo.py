# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
import re

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("wordcount2").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    # 1. SQL 风格进行处理
    rdd = sc.textFile("hdfs://hadoop202:8020/data/input/words.txt").\
        flatMap(lambda x: re.split("\s+", x)).\
        map(lambda x: [x])
    rdd.cache()
    print(rdd.collect())

    df = rdd.toDF(["word"])

    # 注册DF为临时表格
    df.createTempView("words")
    spark.sql("SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()

    # 2. DSL 风格进行处理
    df = spark.read.format("text").load("hdfs://hadoop202:8020/data/input/words.txt")
    df.cache()
    df.show()
    # withColumn 方法
    # 方法功能，对已存在的列进行操作，返回一个新的列，如果名字和老列相同，那么替换，否则作为新的列存在
    df2 = df.withColumn("value", F.explode(F.split(df["value"], " ")))
    df2.cache()
    df2.groupBy("value").\
        count().\
        withColumnRenamed("value", "word").\
        withColumnRenamed("count", "cnt").\
        orderBy("cnt", ascending=False).\
        show()
