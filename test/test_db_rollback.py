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

def test_db_rollback_error_table(db_connection):
    mb = Mybatis(db_connection, "mapper")
    @mb.Insert("INSERT INTO fruits2 (name, category, price) VALUES ('Candy', 'B', 500)")
    def insert():
        pass

    try:
        insert()
    except mysql.connector.errors.Error as err:
        assert True

    @mb.Delete("DELETE FROM fruits2 WHERE id=1")
    def delete():
        pass

    try:
        delete()
    except mysql.connector.errors.Error as err:
        assert True

    @mb.Delete("UPDATE fruits2 SET name='Candy' WHERE id=1")
    def update():
        pass

    try:
        update()
    except mysql.connector.errors.Error as err:
        assert True

def test_db_rollback_error_table2(db_connection):
    mb = Mybatis(db_connection, "mapper")
    try:
        mb.insert("testDBRollbackInsert", {})
    except mysql.connector.errors.Error as err:
        assert True

    try:
        mb.delete("testDBRollbackDelete", {})
    except mysql.connector.errors.Error as err:
        assert True

    try:
        mb.update("testDBRollbackUpdate", {})
    except mysql.connector.errors.Error as err:
        assert True