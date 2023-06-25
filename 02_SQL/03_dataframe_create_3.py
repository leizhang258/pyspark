# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("df_create_3").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile("hdfs://hadoop202:8020/data/input/sql/people.txt").map(lambda x: x.split(",")).map(lambda x: (x[0],int(x[1])))

    schema = StructType().\
        add("name", StringType(), nullable=False).\
        add("age", IntegerType(), nullable=False)

    # 方式1：只传列名，类型靠推断，是否云讯为空时true
    df1 = rdd.toDF(["name", "age"])
    df1.printSchema()
    df1.show()

    # 方式2：传入完整的schema描述对象StructType
    df2 = rdd.toDF(schema)
    df2.printSchema()
    df2.show()

