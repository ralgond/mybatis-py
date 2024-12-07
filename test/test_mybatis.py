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

def test_select_one(db_connection):
    mb = Mybatis(db_connection, "mapper", cache_memory_limit=50*1024*1024)
    ret = mb.select_one('testBasic', {})
    assert ret is not None
    assert len(ret) == 4
    assert ret['id'] == 1
    assert ret['name'] == 'Alice'
    assert ret['category'] == 'A'
    assert ret['price'] == 100

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

def test_select_many(db_connection):
    mb = Mybatis(db_connection, "mapper", cache_memory_limit=50*1024*1024)
    ret = mb.select_many('testBasicMany', {})
    assert ret is not None
    assert isinstance(ret, list)
    assert len(ret) == 2
    assert ret[0]['id'] == 1
    assert ret[0]['name'] == 'Alice'
    assert ret[0]['category'] == 'A'
    assert ret[0]['price'] == 100
    assert ret[1]['id'] == 2
    assert ret[1]['name'] == 'Bob'
    assert ret[1]['category'] == 'B'
    assert ret[1]['price'] == 200

    ret = mb.select_many('testBasicMany', {})
    assert ret is not None
    assert isinstance(ret, list)
    assert len(ret) == 2
    assert ret[0]['id'] == 1
    assert ret[0]['name'] == 'Alice'
    assert ret[0]['category'] == 'A'
    assert ret[0]['price'] == 100
    assert ret[1]['id'] == 2
    assert ret[1]['name'] == 'Bob'
    assert ret[1]['category'] == 'B'
    assert ret[1]['price'] == 200

def test_select_many_none(db_connection):
    mb = Mybatis(db_connection, "mapper")
    ret = mb.select_many('testBasicNone', {})
    assert ret is None

def test_update(db_connection):
    mb = Mybatis(db_connection, "mapper")
    mb.select_one('testBasic', {})

    assert mb.cache.empty() is True

    ret = mb.update("testUpdate", {"name":"Candy", "id":2})

    assert mb.cache.empty() is True

    assert ret == 1
    ret = mb.select_many('testBasicMany', {})
    assert ret is not None
    assert isinstance(ret, list)
    assert len(ret) == 2
    assert ret[0]['id'] == 1
    assert ret[0]['name'] == 'Alice'
    assert ret[0]['category'] == 'A'
    assert ret[0]['price'] == 100
    assert ret[1]['id'] == 2
    assert ret[1]['name'] == 'Candy'
    assert ret[1]['category'] == 'B'
    assert ret[1]['price'] == 200

    assert mb.cache.empty() is True


def test_update_with_cache(db_connection):
    mb = Mybatis(db_connection, "mapper", cache_memory_limit=50*1024*1024)
    mb.select_one('testBasic', {})

    assert mb.cache.empty() is False

    ret = mb.update("testUpdate", {"name":"Candy", "id":2})

    assert mb.cache.empty() is True

    assert ret == 1
    ret = mb.select_many('testBasicMany', {})
    assert ret is not None
    assert isinstance(ret, list)
    assert len(ret) == 2
    assert ret[0]['id'] == 1
    assert ret[0]['name'] == 'Alice'
    assert ret[0]['category'] == 'A'
    assert ret[0]['price'] == 100
    assert ret[1]['id'] == 2
    assert ret[1]['name'] == 'Candy'
    assert ret[1]['category'] == 'B'
    assert ret[1]['price'] == 200

    assert mb.cache.empty() is False

def test_delete(db_connection):
    mb = Mybatis(db_connection, "mapper")
    ret = mb.delete("testDelete", {"id":2})
    assert ret == 1
    ret = mb.select_many('testBasicMany', {})
    assert ret is not None
    assert isinstance(ret, list)
    assert len(ret) == 1
    assert ret[0]['id'] == 1
    assert ret[0]['name'] == 'Alice'
    assert ret[0]['category'] == 'A'
    assert ret[0]['price'] == 100

def test_insert(db_connection):
    mb = Mybatis(db_connection, "mapper")
    ret = mb.insert("testInsert", {"name": "Candy", "category": "B", "price": 200})
    assert ret == 3

    ret = mb.select_many('testBasicMany', {})
    assert ret is not None
    assert isinstance(ret, list)
    assert len(ret) == 3
    assert ret[0]['id'] == 1
    assert ret[0]['name'] == 'Alice'
    assert ret[0]['category'] == 'A'
    assert ret[0]['price'] == 100
    assert ret[1]['id'] == 2
    assert ret[1]['name'] == 'Bob'
    assert ret[1]['category'] == 'B'
    assert ret[1]['price'] == 200
    assert ret[2]['id'] == 3
    assert ret[2]['name'] == 'Candy'
    assert ret[2]['category'] == 'B'
    assert ret[2]['price'] == 200