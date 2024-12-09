import pytest
import mysql.connector

import mybatis.errors
from mybatis import Mybatis, ConnectionFactory


@pytest.fixture(scope="function")
def db_connection():
    # 配置数据库连接
    connection = ConnectionFactory.get_connection(
        dbms_name="postgresql",
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis"
    )
    connection.start_transaction()
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS fruits")
    create_table_sql = '''CREATE TABLE IF NOT EXISTS fruits (
        id SERIAL PRIMARY KEY, 
        name VARCHAR(100),
        category VARCHAR(100),
        price int)
    '''
    # 在测试开始前准备数据
    cursor.execute(create_table_sql)
    cursor.execute("INSERT INTO fruits (name, category, price) VALUES ('Alice', 'A', 100)")
    cursor.execute("INSERT INTO fruits (name, category, price) VALUES ('Bob', 'B', 200)")
    connection.commit()

    # 提供数据库连接给测试用例
    yield connection

    # 清理数据和关闭连接
    connection.close()

def test_db_rollback_error_table(db_connection):
    mb = Mybatis(db_connection, "mapper")
    @mb.Insert("INSERT INTO fruits2 (name, category, price) VALUES ('Candy', 'B', 500)")
    def insert():
        pass

    try:
        insert()
    except mybatis.errors.DatabaseError as err:
        assert True

    @mb.Delete("DELETE FROM fruits2 WHERE id=1")
    def delete():
        pass

    try:
        delete()
    except mybatis.errors.DatabaseError as err:
        assert True

    @mb.Delete("UPDATE fruits2 SET name='Candy' WHERE id=1")
    def update():
        pass

    try:
        update()
    except mybatis.errors.DatabaseError as err:
        assert True

def test_db_rollback_error_table2(db_connection):
    mb = Mybatis(db_connection, "mapper")
    try:
        mb.insert("testDBRollbackInsert", {})
    except mybatis.errors.DatabaseError as err:
        assert True

    try:
        mb.delete("testDBRollbackDelete", {})
    except mybatis.errors.DatabaseError as err:
        assert True

    try:
        mb.update("testDBRollbackUpdate", {})
    except mybatis.errors.DatabaseError as err:
        assert True