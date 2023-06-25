# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("udf").\
        master("yarn").\
        config("spark.sql.shuffle.partitions", 9).\
        getOrCreate()
    sc = spark.sparkContext

    # 构建一个RDD
    rdd = sc.parallelize([1,2,3,4,5,6,7]).map(lambda x:[x])
    df = rdd.toDF(["num"])

    # 构建 UDF
    # TODO 1: 方式1 sparksession.udf.register()
    def num_ride_10(num):
        return num * 10
    # 参数1：注册UDF的名称，这个UDF名称，仅可以用于SQL风格
    # 参数2：UDF的处理逻辑，是一个单独的方法
    # 参数3： UDF的返回值类型,注意：UDF注册的时候，必须 声明返回值类型，并且UDF的真实返回值一定要和声明的返回值一致
    # 返回值对象：这是一个UDF对象，仅可以用于DSL语法
    # 当前这种方式定义的UDF，可以 通过参数1的名称用于SQL风格，通过返回值对象用于DSL风格
    udf2 = spark.udf.register("udf1", num_ride_10, IntegerType())

    # 通过SQL风格使用
    # selectExpr以Select的表达式执行，表达式SQL风格的表达式(字符串)
    # select方法，接收普通的字符串字段名，或者返回值是Column对象的计算
    df.selectExpr("udf1(num)").show()

    # 通过DSL风格使用
    # 返回值UDF对象，如果作为方法使用，传入参数一定是Column对象
    df.select(udf2(df["num"])).show()

    # 构建 UDF
    # TODO 2: 方式2 仅能用于DSL风格
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df["num"])).show()