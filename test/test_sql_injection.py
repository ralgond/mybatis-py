import pytest

from mybatis import Mybatis
from mybatis import ConnectionFactory

@pytest.fixture(scope="function")
def db_connection():
    connection = ConnectionFactory.get_connection(
            dbms_name='postgresql',
            host="localhost",
            user="mybatis",
            password="mybatis",
            database="mybatis"
        )
    connection.start_transaction()
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS fruits")
    create_table_sql1 = '''CREATE TABLE IF NOT EXISTS fruits (
        id SERIAL PRIMARY KEY, 
        name VARCHAR(100),
        category VARCHAR(100),
        price int)
    '''
    create_table_sql2 = '''CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY, 
        name VARCHAR(100),
        password VARCHAR(100))
    '''
    # 在测试开始前准备数据
    cursor.execute(create_table_sql1)
    cursor.execute(create_table_sql2)
    cursor.execute("INSERT INTO fruits (name, category, price) VALUES ('Alice', 'A', 100)")
    cursor.execute("INSERT INTO fruits (name, category, price) VALUES ('Bob', 'B', 200)")
    cursor.execute("INSERT INTO users (name, password) VALUES ('Bob', 'B')")
    connection.commit()

    # 提供数据库连接给测试用例
    yield connection

    # 清理数据和关闭连接
    connection.close()

def test_sql_injection1(db_connection):
    mb = Mybatis(db_connection, "mapper", cache_memory_limit=50*1024*1024)

    @mb.SelectMany("SELECT name, category, price FROM fruits WHERE name = #{name}")
    def select_fruit(name):
        pass

    ret = select_fruit(name="'OR '1'='1")
    assert ret is None

    ret = select_fruit(name="OR '1'='1'")
    assert ret is None

def test_sql_injection2(db_connection):
    mb = Mybatis(db_connection, "mapper", cache_memory_limit=50*1024*1024)

    @mb.SelectMany("SELECT name, category, price FROM fruits WHERE name = #{name}")
    def select_fruit(name):
        pass

    ret = select_fruit(name=" '' UNION SELECT name, password, 1 FROM users")
    assert ret is None