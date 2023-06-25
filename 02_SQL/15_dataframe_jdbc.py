# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("data cleansing").\
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

    # 1.写出DataFrame到MySQL数据库中
    # JDBC 写出会自动在数据库中创建表
    df.write.mode("overwrite").\
        format("jdbc").\
        option("url", "jdbc:mysql://hadoop202:3306/my_db?useSSL=false&useUnicode=true").\
        option("dbtable", "movie_data").\
        option("user", "root").\
        option("password", "000000").\
        save()

    df2 = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://hadoop202:3306/my_db?useSSL=false&useUnicode=true"). \
        option("dbtable", "movie_data"). \
        option("user", "root"). \
        option("password", "000000").\
        load()
    df2.printSchema()
    df2.show()

