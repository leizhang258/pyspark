# coding:utf8
import string

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("udf define return dict").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    # 假设有三个数字 1,2,3；我们传入数字，返回数字所在序号对应的字母，然后和数字结合组成字典返回
    # 例如：传入数字1，则返回 {"num": 1, "lettter":"a"}
    rdd = sc.parallelize([[1],[2],[3]])
    df = rdd.toDF(["num"])

    # 折中的方式，就是使用RDD的mapPartitions算子来实现聚合操作
    # 如果用mapPartitions API完成UDAF的聚合操作，一定要单分区
    single_partition_rdd = df.rdd.repartition(1)

    def process(iter):
        sum = 0
        for row in iter:
            sum += row["num"]
        return [sum] # return 的时候一定要嵌套list。因为mapPartitions方法要求返回值是list对象

    print(single_partition_rdd.mapPartitions(process).collect())