import pytest
import mysql.connector

from mybatis import Mybatis, MySQLConnection, ConnectionFactory

@pytest.fixture(scope="function")
def mysql_db_connection():
    # 配置数据库连接
    connection = ConnectionFactory.get_connection(dbms_name="mysql",
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis",
        autocommit=False,
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
def mysql_db_connection2():
    # 配置数据库连接
    connection = ConnectionFactory.get_connection(dbms_name="mysql",
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis",
        autocommit=False,
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
    connection = ConnectionFactory.get_connection(dbms_name="postgresql",
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
def postgresql_db_connection2():
    # 配置数据库连接
    connection = ConnectionFactory.get_connection(dbms_name="postgresql",
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

def test_mysql_connection(mysql_db_connection):
    # mysql_connection = MySQLConnection(mysql_db_connection)
    with mysql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits")
        res = cursor.fetchall()
        assert res == [(1, 'Alice', 'A', 100), (2, 'Bob', 'B', 200)]

def test_mysql_connection_one(mysql_db_connection):
    # mysql_connection = MySQLConnection(mysql_db_connection)
    with mysql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits WHERE id = ?", (1,))
        res = cursor.fetchall()
        assert res == [(1, 'Alice', 'A', 100)]

def test_mysql_connection_read_new(mysql_db_connection, mysql_db_connection2):
    mysql_db_connection.start_transaction()
    with mysql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits")
        res = cursor.fetchall()
        mysql_db_connection.commit()
        assert (res == [(1, 'Alice', 'A', 100), (2, 'Bob', 'B', 200)])

    mysql_db_connection2.start_transaction()
    with mysql_db_connection2.cursor(prepared=True) as cursor:
        cursor.execute("INSERT INTO fruits (name, category, price) VALUES ('Bob', 'B', 100)");
        mysql_db_connection2.commit()
        res = cursor.fetchall()

    mysql_db_connection2.start_transaction()
    with mysql_db_connection2.cursor(prepared=True) as cursor:
        cursor.execute("UPDATE fruits SET NAME='Amazon' WHERE id=?", (1,))
        mysql_db_connection2.commit()

    mysql_db_connection2.start_transaction()
    with mysql_db_connection2.cursor(prepared=True) as cursor:
        cursor.execute("DELETE FROM fruits WHERE id=?", (2,))
        mysql_db_connection2.commit()

    with mysql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits ORDER BY id")
        res = cursor.fetchall()
        assert res == [(1, 'Amazon', 'A', 100), (3, 'Bob', 'B', 100)]


def test_postgresql_connection(postgresql_db_connection):
    with postgresql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits")
        res = cursor.fetchall()
        assert res == [(1, 'Alice', 'A', 100), (2, 'Bob', 'B', 200)]

def test_postgresql_connection_one(postgresql_db_connection):
    with postgresql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits WHERE id = ?", (1,))
        res = cursor.fetchall()
        assert res == [(1, 'Alice', 'A', 100)]

def test_postgresql_connection_read_new(postgresql_db_connection, postgresql_db_connection2):
    postgresql_db_connection.start_transaction()
    with postgresql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits")
        res = cursor.fetchall()
        postgresql_db_connection.commit()
        assert (res == [(1, 'Alice', 'A', 100), (2, 'Bob', 'B', 200)])

    postgresql_db_connection2.start_transaction()
    with postgresql_db_connection2.cursor(prepared=True) as cursor:
        cursor.execute("INSERT INTO fruits (name, category, price) VALUES ('Bob', 'B', 100)");
        postgresql_db_connection2.commit()

    postgresql_db_connection2.start_transaction()
    with postgresql_db_connection2.cursor(prepared=True) as cursor:
        cursor.execute("UPDATE fruits SET NAME='Amazon' WHERE id=?", (1,))
        postgresql_db_connection2.commit()

    postgresql_db_connection2.start_transaction()
    with postgresql_db_connection2.cursor(prepared=True) as cursor:
        cursor.execute("DELETE FROM fruits WHERE id=?", (2,))
        postgresql_db_connection2.commit()

    with postgresql_db_connection.cursor(prepared=True) as cursor:
        cursor.execute("SELECT * FROM fruits ORDER BY id")
        res = cursor.fetchall()
        assert res == [(1, 'Amazon', 'A', 100), (3, 'Bob', 'B', 100)]