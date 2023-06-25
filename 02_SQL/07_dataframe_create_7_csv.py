# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("df_create_7_csv").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    schema = StructType().\
        add("name", StringType(), nullable=True).\
        add("age", IntegerType(), nullable=True).\
        add("job", StringType(), nullable=True)

    # schema可以指定为 .schema(schema=schema)\
    # schema也可以 .schema("name STRING, age INT, job STRING")
    df = spark.read.format("csv")\
        .option("sep", ";")\
        .option("header", False)\
        .option("encoding", "utf-8")\
        .schema("name STRING, age INT, job STRING")\
        .load("hdfs://hadoop202:8020/data/input/sql/people.csv")

    df.printSchema()
    df.show()