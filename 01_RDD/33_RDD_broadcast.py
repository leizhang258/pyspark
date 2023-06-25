# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("boardcast").setMaster("yarn")
    sc = SparkContext(conf=conf)

    stu_info_list = [(1, 'kevin', 11),
                      (2, 'jim', 13),
                      (3, 'mark', 11),
                      (4, 'anna', 11)]
    # 1. 将本地python list标记为广播变量
    broadcast = sc.broadcast(stu_info_list)

    score_info_rdd = sc.parallelize([
        (1, 'reading', 99),
        (2, 'math', 99),
        (3, 'english', 99),
        (4, 'computer', 99),
        (1, 'reading', 99),
        (2, 'computer', 99),
        (3, 'reading', 99),
        (4, 'english', 99),
        (1, 'reading', 99),
        (3, 'english', 99),
        (2, 'computer', 99)
    ])

    def map_func(data):
        id = data[0]
        name = ""
        # for stu_info in  stu_info_list:
        # 2.取出广播变量
        for stu_info in broadcast.value:
            stu_id = stu_info[0]
            if stu_id == id:
                name = stu_info[1]
        return (name, data[1], data[2])

    print(score_info_rdd.map(map_func).collect())