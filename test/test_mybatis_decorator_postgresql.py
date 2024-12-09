import pytest

from mybatis import Mybatis, ConnectionFactory


@pytest.fixture(scope="function")
def postgresql_db_connection():
    # 配置数据库连接
    # connection = mysql.connector.connect(
    #     host="localhost",
    #     user="mybatis",
    #     password="mybatis",
    #     database="mybatis",
    #     autocommit=False,
    # )
    connection = ConnectionFactory.get_connection(
            dbms_name='postgresql',
            host="localhost",
            user="mybatis",
            password="mybatis",
            database="mybatis",
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

def test_select_one(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper", cache_memory_limit=50*1024*1024)

    @mb.SelectOne("SELECT * FROM fruits WHERE id=#{id}")
    def select_one(id:int):
        pass

    ret = select_one(id=1)

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

def test_select_one_none(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper")

    @mb.SelectOne("SELECT * FROM fruits WHERE id=#{id}")
    def select_one(id: int):
        pass

    ret = select_one(id=3)
    assert ret is None

def test_select_many(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper", cache_memory_limit=50*1024*1024)
    @mb.SelectMany("SELECT * FROM fruits")
    def select_many():
        pass

    ret = select_many()
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


def test_select_many_none(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper")
    @mb.SelectMany("SELECT * FROM fruits WHERE id=5")
    def select_many():
        pass

    ret = select_many()
    assert ret is None


def test_update(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper")

    @mb.SelectMany("SELECT * FROM fruits")
    def select_many():
        pass

    @mb.Update("UPDATE fruits SET name=#{name} WHERE id=#{id}")
    def update(name:str, id:int):
        pass

    assert mb.cache.empty() is True

    ret = update(name="Candy", id=2)

    assert mb.cache.empty() is True

    assert ret == 1
    ret = select_many()
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

def test_update_with_cache(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper", cache_memory_limit=50*1024*1024)

    @mb.SelectMany("SELECT * FROM fruits")
    def select_many():
        pass

    @mb.SelectOne("SELECT * FROM fruits WHERE id=1")
    def select_one():
        pass

    @mb.Update("UPDATE fruits SET name=#{name} WHERE id=#{id}")
    def update(name: str, id: int):
        pass

    select_one()

    assert mb.cache.empty() is False

    ret = update(name="Candy", id=2)

    assert mb.cache.empty() is True

    assert ret == 1
    ret = select_many()
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

def test_delete(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper")

    @mb.SelectMany("SELECT * FROM fruits")
    def select_many():
        pass

    @mb.Delete("DELETE FROM fruits WHERE id=#{id}")
    def delete(id:int):
        pass

    ret = delete(id=2)

    assert ret == 1
    ret = select_many()
    assert ret is not None
    assert isinstance(ret, list)
    assert len(ret) == 1
    assert ret[0]['id'] == 1
    assert ret[0]['name'] == 'Alice'
    assert ret[0]['category'] == 'A'
    assert ret[0]['price'] == 100

def test_insert(postgresql_db_connection):
    mb = Mybatis(postgresql_db_connection, "mapper", postgresql_primary_key_name="id")

    @mb.SelectMany("SELECT * FROM fruits")
    def select_many():
        pass

    @mb.Insert("INSERT INTO fruits (name, category, price) VALUES (#{name}, #{category}, #{price})")
    def insert(name:str, category:str, price:int):
        pass

    ret = insert(name="Candy", category="B", price=200)

    assert ret == 3

    ret = select_many()
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