# coding:utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("create_df").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    # 基于RDD转换成DataFrame
    rdd  = sc.textFile("hdfs://hadoop202:8020/data/input/sql/people.txt").\
        map(lambda line:line.split(",")).\
        map(lambda x: [x[0], int(x[1])])

    rdd.cache()
    print(rdd.collect())

    # 通过RDD来构建DataFrame对象
    # 参数1 被转换的RDD
    # 参数2 指定列明，通过list的形式指定，按照顺序依次提供字符串名称即可
    df = spark.createDataFrame(rdd, schema=["name","age"])

    # 打印DataFrame的表结构
    df.printSchema()
    # 参数1 表示展示多少条数据，默认20
    # 参数2 表示是否进行截断，默认是True（截断显示）
    df.show(3)

    df.createTempView("people")
    spark.sql("""
    SELECT * FROM people WHERE age <30
    """).show()

