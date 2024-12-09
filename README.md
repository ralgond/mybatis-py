# mybatis-py
[![Latest Version](https://img.shields.io/pypi/v/mybatis.svg)](https://pypi.org/project/mybatis/)
[![codecov](https://codecov.io/gh/ralgond/mybatis-py/branch/main/graph/badge.svg)](https://codecov.io/gh/ralgond/mybatis-py)


A python ORM like mybatis.

## How to Use

### Install 
pip install -U mybatis

### Create Database
```sql
CREATE DATABASE mybatis;

USE mybatis;

CREATE TABLE IF NOT EXISTS fruits (
        id INT AUTO_INCREMENT PRIMARY KEY, 
        name VARCHAR(100),
        category VARCHAR(100),
        price int)

INSERT INTO fruits (name, category, price) VALUES ('Alice', 'A', 100)
INSERT INTO fruits (name, category, price) VALUES ('Bob', 'B', 200)
```

### Write Code

refer to [test_mybatis.py](https://github.com/ralgond/mybatis-py/blob/main/test/test_mybatis.py)、[test2.xml](https://github.com/ralgond/mybatis-py/blob/main/mapper/test.xml)

Create a mapper directory, and create a file named mapper/test.xml, as follows:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper>
    <select id="testBasic1">
        SELECT * from fruits where id=#{id}
    </select>
</mapper>
```
Create a Python file named "test.py" as follows:
```python
from mybatis import *
import mysql.connector

def main():
    conn = mysql.connector.connect(
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis"
    )

    mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024)

    ret = mb.select_one("testBasic1", {'id':1})

    print(ret)

if __name__ == "__main__":
    main()
```

## Decorator
The example is as follows:
```python
import mysql.connector
from mybatis import Mybatis

conn = mysql.connector.connect(
    host="localhost",
    user="mybatis",
    password="mybatis",
    database="mybatis"
)

mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024)

@mb.SelectOne("SELECT * FROM fruits WHERE id=#{id}")
def get_one(id:int):
    pass

@mb.SelectMany("SELECT * FROM fruits")
def get_many():
    pass

@mb.Insert("INSERT INTO fruits (name, category, price) VALUES (#{name}, #{category}, #{price})")
def insert(name:str, category:str, price:int):
    pass

@mb.Delete("DELETE FROM fruits WHERE id=#{id}")
def delete(id:int):
    pass

@mb.Update("UPDATE fruits SET name=#{name} WHERE id=#{id}")
def update(name:str, id:int):
    pass

print(get_one(id=1))

print(delete(id=4))

print(get_many())

print(insert(name="Dating", category="D", price=20))

print(get_many())

print(update(name='Amazon', id=1))

print(get_many())
```

## Dynamic SQL

### Namespace
Create xml mapper as follows:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="test_namespace">
    <insert id="testInsert">
        INSERT INTO fruits (name, category, price) VALUES (#{name},#{category},#{price})
    </insert>
    <delete id="testDelete">
        DELETE FROM fruits WHERE id=#{id}
    </delete>
    <update id="testUpdate">
        UPDATE fruits SET name=#{name} WHERE id=#{id}
    </update>
</mapper>
```
Write python code as follows:
```python
from mybatis import MapperManager

mm = MapperManager()
mm.read_mapper_xml_file("mapper/test_namespace.xml")

sql, param_list = mm.insert("test_namespace.testInsert", {'name': 'Alice', 'category': 'A', 'price': 100})
assert sql == "INSERT INTO fruits (name, category, price) VALUES (?,?,?)"
```

### The difference between ${} and #{} 
#{} is a placeholder that exists for prepared statement and will become the character '?' after processing by MapperManager.
${} represents simple string replacement. The following example illustrates the difference:
```python
from mybatis import *

mm = MapperManager()

'''
The contents of test.xml are as follows:

<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper>
    <select id="testStringReplace">
        SELECT * from fruits_${date} where id=#{id}
    </select>
</mapper>
'''
mm.read_mapper_xml_file("mapper/test.xml")

sql, param_list = mm.select("testStringReplace", {'id':1, 'date':"20241204"})
print(sql, param_list)
```
The result is as follows:
```bash
SELECT * from fruits_20241204 where id=? [1]
```
You can see that ${date} is replaced with "20241204", and #{id} is replaced with '?', and only one parameter value in param_list is 1.

Based on security considerations, in order to prevent SQL injection, it is recommended not to use ${} as long as #{} can be used, unless you are confident enough.

## Cache
mybatis-py maintains a cache pool for each connection. The elimination strategy is LRU. You can define the maximum byte capacity of the pool. If you do not want to use cache, you can set the parameter configuration. The code is as follows:
```python
from mybatis import *

def main():
    conn1 = ConnectionFactory.get_connection(
        dbms_name="mysql",
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis"
    )

    conn2 = ConnectionFactory.get_connection(
        dbms_name="mysql",
        host="localhost",
        user="mybatis",
        password="mybatis",
        database="mybatis"
    )

    mb1 = Mybatis(conn1, "mapper", cache_memory_limit=50*1024*1024) # Capacity limit is 50MB
    mb2 = Mybatis(conn2, "mapper", cache_memory_limit=None) # Disable caching
```
### Timeout mechanism
In order to prevent users from always getting old data, the cache will determine whether the key-value has expired when fetching a key-value. The maximum life milliseconds of the key-value can be specified through the ```cache_max_live_ms``` parameter in the constructor of the Mybatis class.

## Use in Flask
### Auto Reconnecting
```python
from flask import Flask

import mybatis.errors
from mybatis import Mybatis, ConnectionFactory
import orjson as json
import functools

app = Flask(__name__)


# 连接到 MySQL 数据库
conn = None
mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024)
connection_error = False
error_string = ""

def make_connection_and_mybatis():
    global conn
    global mb
    global connection_error
    global error_string

    if conn is None:
        try:
            conn = ConnectionFactory.get_connection(
                            dbms_name="postgresql",
                            host="localhost",
                            user="mybatis",  
                            password="mybatis", 
                            database="mybatis"
            )
            mb.conn = conn
            mb.conn.set_autocommit(False)
                
            return True
            
        except Exception as e:
            connection_error = True
            error_string = str(e)
            return False
    else:
        try:
            if connection_error:
                conn.reconnect(3, 3)
                connection_error = False

            if mb.conn is None:
                mb.conn = conn
            return True
        except Exception as e:
            connection_error = True
            error_string = str(e)
            return False


@mb.SelectOne("SELECT * FROM fruits WHERE id=#{id}")
def select_one(id:int):
    pass

@mb.SelectMany("SELECT * FROM fruits")
def select_many():
    pass

@mb.Insert("INSERT INTO fruits (name,category,price) VALUES ('Candy', 'C', 500)")
def insert():
    pass

def sql_auto_reconnect(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global connection_error
        try:
            ret = make_connection_and_mybatis()
            if ret is False:
                return error_string, 500

            ret = func(*args, **kwargs)
            return ret, 200

        except mybatis.errors.DatabaseError as e:
            connection_error = True
            return str(e), 500
        except Exception as e:
            return str(e), 500

    return wrapper


@app.route('/')
@sql_auto_reconnect
def hello():
    ret = select_many()
    return json.dumps(ret)


@app.route('/insert')
@sql_auto_reconnect
def do_insert():
    ret = insert()
    return json.dumps(ret)

if __name__ == "__main__":
    app.run(debug=True)

```
