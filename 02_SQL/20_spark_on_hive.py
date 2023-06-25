# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("window function").\
        master("yarn").\
        config("spark.sql.shuffle.partitions", 9).\
        config("spark.sql.warehouse.dir", "hdfs://hadoop202:8020/user/hive/warehouse").\
        config("hive.metastore.uris", "thrift://hadoop202:9083").\
        enableHiveSupport().\
        getOrCreate()
    sc = spark.sparkContext

    spark.sql("SELECT * FROM spark_hive_test").show()