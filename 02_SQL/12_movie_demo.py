# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F
import time
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("movie").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    # 1. 读取数据集
    schema = StructType().\
        add("user_id", StringType(), nullable=True).\
        add("movie_id", StringType(), nullable=True).\
        add("rank", IntegerType(), nullable=True).\
        add("ts", IntegerType(), nullable=True)

    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("hdfs://hadoop202:8020/data/input/sql/u.data")
    df.cache()

    # TODO 1: 用户平均分
    df.groupBy("user_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank",2)).\
        orderBy("avg_rank", ascending=False).\
        show(10)
    # TODO 2: 电影平均分
    df.createTempView("movie")
    spark.sql("SELECT movie_id, AVG(rank) avg_rank FROM movie GROUP BY movie_id ORDER BY avg_rank DESC").show(10)

    # TODO 3: 查询大于平均分的电影数量
    df.where(df["rank"] > df.select(F.avg(df["rank"])).first()["avg(rank)"]).count()

    # TODO 4: 查询高分电影中(>3)打分次数最多的用户，并求出此人打的平均分
    # 先找出打分最高的人
    user_id = df.where("rank > 3").\
        groupBy("user_id").\
        count().\
        withColumnRenamed("count","cnt").\
        orderBy("cnt", ascending=False).\
        limit(1).\
        first()["user_id"]
    # 计算这个人的打分平均分
    df.filter(df["user_id"] == user_id).\
        select(F.round(F.avg("rank"),2)).show()

    # TODO 5: 查询每个用户的平均打分，最低打分，最高打分
    df.groupBy("user_id").\
        agg(
        F.round(F.avg("rank"),2).alias("avg_rank"),
        F.min("rank").alias("min_rank"),
        F.max("rank").alias("max_rank")
    ).show()

    # TODO 6: 查询 平分超过100次的电影的平均分，排名 TOP 10
    df.groupBy("movie_id").\
        agg(
        F.count("movie_id").alias("cnt"),
        F.round(F.avg("rank")).alias("avg_rank")
    ).where("cnt > 100").\
        orderBy("avg_rank", ascending=True).\
        show()
    time.sleep(100)