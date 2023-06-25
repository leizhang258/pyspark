# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("yarn")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('c',1), ('b',1), ('a',1), ('m', 2), ('o',1), ('b',1), ('y',1), ('z', 2)])
    #sortBy(v1,v2,v3)
    # 参数v1是指定按照数据中的哪个列进行排序
    # 参数v2是指定升序还是降序，true是升序，false是降序，默认升序
    # 参数v3是指定排序的总分区数,注意生产环境中如果设置大于1，可能导致分区内是有序但collect后是无序的。生产环境请设置=1.
    sort_rdd = rdd.sortBy(lambda x:x[0], ascending=True, numPartitions=1)
    print(sort_rdd.collect())