# coding:utf8

# SparkSession对象的导包，对象是来自于pyspark.sql包中
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建 SparkSession执行环境入口对象
    spark = SparkSession.builder.\
        appName("session_test").\
        master("local[*]").\
        getOrCreate()

    # 通过SparkSession对象，获取SparkContext对象
    sc = spark.sparkContext

    # SparkSQL的HelloWorld
    df = spark.read.csv("hdfs://hadoop202:8020/data/input/stu_score.txt", sep=",", header=False)
    df2 = df.toDF("id","name","score")
    df2.printSchema()
    df2.show(2)

    df2.createTempView("score")
    # SQL 风格
    spark.sql("""
        SELECT * FROM score WHERE name = '英语' LIMIT 2
        """).show()

    # DSL 风格
    df2.where("name='数学'").limit(2).show()
