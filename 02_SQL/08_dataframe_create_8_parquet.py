# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("df_create_parquet").\
        master("yarn").\
        getOrCreate()

    df = spark.read.format("parquet").load("hdfs://hadoop202:8020/data/input/sql/users.parquet")
    df.printSchema()
    df.show()