import pytest
import mysql.connector

import mybatis.errors
from mybatis import Mybatis, ConnectionFactory


@pytest.fixture(scope="function")
def mysql_db_connection():
    # 配置数据库连接
    connection = ConnectionFactory.get_connection(
        dbms_name="mysql",
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis"
    )
    connection.start_transaction()
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS fruits")
    create_table_sql = '''CREATE TABLE IF NOT EXISTS fruits (
        id INT AUTO_INCREMENT PRIMARY KEY, 
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

@pytest.fixture(scope="function")
def postgresql_db_connection():
    # 配置数据库连接
    connection = ConnectionFactory.get_connection(
        dbms_name="mysql",
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

@pytest.fixture(scope="function")
def sqlite3_db_connection():
    # 配置数据库连接
    connection = ConnectionFactory.get_connection(
        dbms_name="sqlite3",
        db_path="./test.db"
    )
    connection.start_transaction()
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS fruits")
    create_table_sql = '''CREATE TABLE IF NOT EXISTS fruits (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        name VARCHAR,
        category VARCHAR,
        price INTEGER)
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

def test_db_rollback_error_table(mysql_db_connection):
    mb = Mybatis(mysql_db_connection, "mapper")
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

def test_db_rollback_error_table2(mysql_db_connection):
    mb = Mybatis(mysql_db_connection, "mapper")
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


def test_postgresql_db_rollback_error_table(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper")
    @mb.Insert("INSERT INTO fruits2 (name, category, price) VALUES ('Candy', 'B', 500)", primary_key="id")
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

def test_postgresql_db_rollback_error_table2(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper")
    try:
        mb.insert("testDBRollbackInsert", {}, primary_key="id")
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

def test_sqlite3_db_rollback_error_table(sqlite3_db_connection):
    mb = Mybatis(sqlite3_db_connection, "mapper")
    @mb.Insert("INSERT INTO fruits2 (name, category, price) VALUES ('Candy', 'B', 500)", primary_key="id")
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

def test_sqlite3_db_rollback_error_table2(sqlite3_db_connection):
    mb = Mybatis(sqlite3_db_connection, "mapper")
    try:
        mb.insert("testDBRollbackInsert", {}, primary_key="id")
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