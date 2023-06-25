# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("create_df_2").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile("hdfs://hadoop202:8020/data/input/sql/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))

    # 构建表结构的描述，对象：StructType对象
    schema = StructType().\
        add("name", StringType(), nullable=False).\
        add("age", IntegerType(), nullable=True)

    df = spark.createDataFrame(rdd, schema=schema)
    df.printSchema()
    # 参数1 表示展示多少条数据，默认20
    # 参数2 表示是否进行截断，默认是True（截断显示）
    df.show(3,False)

    df.createTempView("people")
    spark.sql("""
    SELECT * FROM people WHERE age <30
    """).show()