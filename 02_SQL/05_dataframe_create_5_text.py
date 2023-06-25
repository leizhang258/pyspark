# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("df_create_api").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    schema = StructType().\
        add("data", StringType(), nullable=True)

    df = spark.read.format("text").\
        schema(schema=schema).\
        load("hdfs://hadoop202:8020/data/input/sql/people.txt")
    df.printSchema()
    df.show()