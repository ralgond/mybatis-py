from mybatis import *

import mysql.connector

# 连接到 MySQL 数据库
conn = mysql.connector.connect(
    host="localhost",         # MySQL 主机地址
    user="root",     # MySQL 用户名
    password="19875522", # MySQL 密码
    database="sakila"  # 需要连接的数据库
)

cur = conn.cursor(prepared=True)

def main():
    mm = MapperManager()

    mm.read_mapper_xml_file("mapper/test.xml")

    # sql, param_list = mm.select("testInclude", {'category':"A", "price":500})
    # print(sql, param_list)

    # sql, param_list = mm.select("testIf", {'category':"A", "price":500})
    # print(sql, param_list)

    # sql, param_list = mm.select("testWhere", {'category': "A", "price": 500})
    # print(sql, param_list)

    sql, param_list = mm.select("testChoose", {'category': "banana", "price": 500})
    print(sql, param_list)

    # cur.execute(sql, param_list, multi=True)
    #
    # print(cur.description)
    #
    # res = cur.fetchall()
    # print(res)

if __name__ == "__main__":
    main()