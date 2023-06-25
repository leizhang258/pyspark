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

    # Write text 写出，只能写出一个列的数据，需要将df转换为单列df
    df.select(F.concat_ws("---","user_id","movie_id","rank","ts")).\
        write.\
        mode("overwrite").\
        format("text").\
        save("hdfs://hadoop202:8020/data/output/sql/movie/text")

    df.write.\
        mode("overwrite").\
        format("csv").\
        option("sep",";").\
        option("header", True). \
        save("hdfs://hadoop202:8020/data/output/sql/movie/csv")

    df.write.\
        mode("overwrite").\
        format("json").\
        save("hdfs://hadoop202:8020/data/output/sql/movie/json")

    df.write.\
        mode("overwrite").\
        format("parquet").\
        save("hdfs://hadoop202:8020/data/output/sql/movie/parquet")