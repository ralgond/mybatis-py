import pytest
import mysql.connector

from mybatis import Mybatis


@pytest.fixture(scope="function")
def db_connection():
    # 配置数据库连接
    connection = mysql.connector.connect(
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis"
    )
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

def test_select_one(db_connection):
    mb = Mybatis(db_connection, "mapper")
    ret = mb.select_one('testBasic', {})
    assert ret is not None
    assert len(ret) == 4
    assert ret['id'] == 1
    assert ret['name'] == 'Alice'
    assert ret['category'] == 'A'
    assert ret['price'] == 100

def test_select_one_none(db_connection):
    mb = Mybatis(db_connection, "mapper")
    ret = mb.select_one('testBasicNone', {})
    assert ret is None