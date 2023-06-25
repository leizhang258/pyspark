from pyhive import hive
if __name__ == '__main__':
    #获取到Hive(Spark ThriftServer的连接）
    conn = hive.Connection(host="hadoop202", port=10000, username="atguigu")

    # 获取一个游标对象
    cursor = conn.cursor()

    # 执行SQL
    cursor.execute("SELECT * FROM spark_hive_test")

    # 通过fetchall API 获取返回值
    result = cursor.fetchall()

    print(result)
