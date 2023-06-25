# coding:utf8
from pyspark import SparkConf, SparkContext
import os

os.environ['HADOOP_CONF_DIR'] = "/opt/module/hadoop-3.3.1/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName("takeOrdered").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,3,4,6,7,5,9])

    print(rdd.takeOrdered(3))

    print(rdd.takeOrdered(3, lambda x: -x))