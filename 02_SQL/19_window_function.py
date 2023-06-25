# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("window function").\
        master("yarn").\
        config("spark.sql.shuffle.partitions", 9).\
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([
        ('kevin', 'class_1', 99),
        ('jack', 'class_2', 87),
        ('mark', 'class_3', 76),
        ('leo', 'class_4', 91),
        ('gap', 'class_5', 39),
        ('fillip', 'class_1', 55),
        ('shawn', 'class_2', 77),
        ('jinx', 'class_3', 78),
        ('jax', 'class_4', 99),
        ('wills', 'class_5', 95),
        ('ann', 'class_1', 89),
        ('king', 'class_2', 90),
        ('lee', 'class_3', 79),
        ('phillip', 'class_4', 80),
        ('Elisa', 'class_5', 93),
        ('simen', 'class_1', 99),
        ('monica', 'class_2', 87),
        ('green', 'class_3', 76),
        ('woods', 'class_4', 91),
        ('hilton', 'class_5', 39),
    ])
    schema = StructType().add("name", StringType()).\
        add("class", StringType()).\
        add("score", IntegerType())
    df = rdd.toDF(schema=schema)

    # TODO 聚合窗口函数
    df.createTempView("stu")
    spark.sql("""
        SELECT *, AVG(score) OVER (partition by class) AS avg_class_score FROM stu
    """).show()

    # TODO 排序相关的窗口函数计算
    # RANK, DENS_RANK, ROW_NUMBER
    spark.sql("""
        SELECT *, 
            RANK() OVER(PARTITION BY class ORDER BY score DESC) AS rank_class_core,
            DENSE_RANK() OVER(PARTITION BY class ORDER BY score DESC) AS dens_rank_class_core,
            ROW_NUMBER() OVER(ORDER BY score DESC) AS row_num
            FROM stu
    """).show(truncate=False)

    #  TODO NTILE
    spark.sql("""
        SELECT *, NTILE(6) OVER(ORDER BY score DESC) AS ntile_score FROM stu
    """).show()