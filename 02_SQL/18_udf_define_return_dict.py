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

    # 注册UDF
    def process(data):
        return {"num": data, "letters": string.ascii_letters[data]}
    # UDF返回值是字典时，需要用到StructType来接受
    udf1 = spark.udf.register("udf1", process, StructType().add("num", IntegerType(), nullable=True).\
                              add("letters", StringType(), nullable=True))
    df.selectExpr("udf1(num)").show(truncate=False) # SQL 风格
    df.select(udf1(df["num"])).show(truncate=False)  # DSL 风格