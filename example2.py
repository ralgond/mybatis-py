from mybatis import *

def main():
    # 连接到 MySQL 数据库
    # conn = mysql.connector.connect(
    #     host="localhost",  # MySQL 主机地址
    #     user="mybatis",  # MySQL 用户名
    #     password="mybatis",  # MySQL 密码
    #     database="mybatis"  # 需要连接的数据库
    # )
    conn = ConnectionFactory.get_connection(dbms_name="postgresql",
                                     host="localhost",
                                     user="mybatis",
                                     password="mybatis",
                                     database="mybatis")

    mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024)

    ret = mb.select_one("testBasic1", {'id':1})

    print(ret)

if __name__ == "__main__":
    main()