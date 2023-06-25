# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("DSL").\
        master("yarn").\
        getOrCreate()
    sc = spark.sparkContext

    schema = StructType().\
        add("id", IntegerType(), nullable=True).\
        add("subject", StringType(), nullable=True).\
        add("score", DecimalType(), nullable=True)

    df = spark.read.format("csv").\
        option("sep", ",").\
        schema(schema=schema).\
        load("hdfs://hadoop202:8020/data/input/sql/stu_score.txt")

    id_col = df['id']
    subject_col = df['subject']

    # DSL 风格演示
    df.select(["id", "subject"]).show()
    df.select("id", "subject").show()
    df.select(id_col, subject_col).show()

    # Filter API
    df.filter("score < 99").show()
    df.filter(df['score'] < 99).show()

    # Where API
    df.where("score < 99").show()
    df.where(df['score'] < 99).show()

    # Group by API
    # 注：groupby的返回是 <class 'pyspark.sql.group.GroupedData'>对象
    df.groupBy("subject").count().show()
    df.groupBy(df["subject"]).avg("score").show()
    print(type(df.groupBy(df["subject"])))

