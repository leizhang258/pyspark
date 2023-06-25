# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("SQL").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    schema = StructType().\
        add("id", IntegerType(), nullable=True).\
        add("subject", StringType(), nullable=True).\
        add("score", DecimalType(), nullable=True)

    df = spark.read.format("csv").\
        option("sep", ",").\
        schema(schema=schema).\
        load("hdfs://hadoop202:8020/data/input/sql/stu_score.txt")

    # 注册成临时表
    df.createTempView("score1") #注册临时视图
    df.createOrReplaceTempView("score2") # 注册或替换临时视图
    df.createGlobalTempView("score3") # 注册成全局临时视图， 全局临时视图使用时，需要在前面加上 global_temp. 前缀

    # 可以通过SparkSession随想的API完成SQL语句的执行
    spark.sql("SELECT subject, AVG(score) AS avg_score FROM score1 GROUP BY subject").show()
    spark.sql("SELECT subject, AVG(score) AS avg_score FROM score2 GROUP BY subject").show()
    spark.sql("SELECT subject, AVG(score) AS avg_score FROM global_temp.score3 GROUP BY subject").show()