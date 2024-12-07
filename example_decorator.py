import mysql.connector
from mybatis import Mybatis

conn = mysql.connector.connect(
    host="localhost",  # MySQL 主机地址
    user="mybatis",  # MySQL 用户名
    password="mybatis",  # MySQL 密码
    database="mybatis",  # 需要连接的数据库,
    autocommit=False
)

mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024)

@mb.SelectOne("SELECT * FROM fruits WHERE id=#{id}")
def get_one(id:int):
    pass

@mb.SelectMany("SELECT * FROM fruits")
def get_many():
    pass

@mb.Insert("INSERT INTO fruits (name, category, price) VALUES (#{name}, #{category}, #{price})")
def insert():
    pass

@mb.Delete("DELETE FROM fruits WHERE id=#{id}")
def delete(id:int):
    pass

@mb.Update("UPDATE fruits SET name='Amazon' WHERE id=#{id}")
def update(id:int):
    pass

print(get_one(id=1))

print(delete(id=4))

print(get_many())

print(insert(name="Dating", category="D", price=20))

print(get_many())

print(update(id=1))

print(get_many())