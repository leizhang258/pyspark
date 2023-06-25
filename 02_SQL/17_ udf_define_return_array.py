# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("udf define return array").\
        master("yarn").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([["hadoop spark flink"], ["hadoop flink java"]])
    df = rdd.toDF(["line"])

    # 注册UDF，EDF执行函数的定义
    def split_line(data):
        return data.split(" ") # 返回值是一个Array对象

    # TODO 1 方式1 构建UDF
    udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))
    # DSL 风格
    df.select(udf2(df['line'])).show()
    # SQL 风格
    df.createTempView("lines")
    spark.sql("""
        SELECT udf1(line) FROM lines
    """).show(truncate=False)

    # TODO 2 方式2 构建UDF（仅适用于DSL风格）
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df["line"])).show(truncate=False)