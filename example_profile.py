from flask import Flask
import mysql.connector
from mybatis import Mybatis
import json

app = Flask(__name__)


# 连接到 MySQL 数据库
conn = mysql.connector.connect(
    host="localhost",  # MySQL 主机地址
    user="mybatis",  # MySQL 用户名
    password="mybatis",  # MySQL 密码
    database="mybatis"  # 需要连接的数据库
)

mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024)

@app.route('/')
def hello():
    ret = mb.select_one("testBasic1", {'id':1})
    return json.dumps(ret)


def main():
    for i in range(100000):
        hello()

if __name__ == "__main__":
    main()