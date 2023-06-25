# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
# Note: Must install pandas==1.5.3 or will have "AttributeError: 'DataFrame' object has no attribute 'iteritems'"
import pandas as pd

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("df_create_4").\
        master("yarn").\
        getOrCreate()

    sc = spark.sparkContext

    pdf = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["kevin", "mark", "david"],
            "age": [21, 23, 40]
        }
    )
    df = spark.createDataFrame(pdf)

    df.printSchema()
    df.show()