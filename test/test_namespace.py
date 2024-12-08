import pytest
import mysql.connector

from mybatis import Mybatis, MapperManager


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


def test_namespace(db_connection):
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test_namespace.xml")

    sql, param_list = mm.insert("test_namespace.testInsert", {'name': 'Alice', 'category': 'A', 'price': 100})
    assert sql == "INSERT INTO fruits (name, category, price) VALUES (?,?,?)"
    assert len(param_list) == 3
    assert param_list[0] == 'Alice'
    assert param_list[1] == 'A'
    assert param_list[2] == 100

    sql, param_list = mm.delete("test_namespace.testDelete", {'id': 1})
    assert sql == "DELETE FROM fruits WHERE id=?"
    assert len(param_list) == 1
    assert param_list[0] == 1

    sql, param_list = mm.update("test_namespace.testUpdate", {'name': 'Bob', 'id':1})
    assert sql == "UPDATE fruits SET name=? WHERE id=?"
    assert len(param_list) == 2
    assert param_list[0] == 'Bob'
    assert param_list[1] == 1




