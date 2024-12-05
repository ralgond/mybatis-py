# mybatis-py
[![Latest Version](https://img.shields.io/pypi/v/mybatis.svg)](https://pypi.org/project/mybatis/)
[![codecov](https://codecov.io/gh/ralgond/mybatis-py/branch/main/graph/badge.svg)](https://codecov.io/gh/ralgond/mybatis-py)


A python ORM like mybatis.

## How to Use

### Install 
pip install mybatis

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

## Dynamic SQL
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
def main():
    # conn = mysql.connector.connect(
    #     host="localhost",
    #     user="mybatis",
    #     password="mybatis",
    #     database="mybatis"
    # )

    mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024) # Capacity limit is 50MB
    mb2 = Mybatis(conn, "mapper", cache_memory_limit=None) # Disable caching
```
