# mybatis-py
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

创建一个mapper目录，创建一个文件mapper/test.xml，如下:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper>
    <select id="testBasic1">
        SELECT * from fruits where id=#{id}
    </select>
</mapper>
```
编写python文件test.py, 如下：
```python
from mybatis import *
import mysql.connector

def main():
    # 连接到 MySQL 数据库
    conn = mysql.connector.connect(
        host="localhost",  # MySQL 主机地址
        user="mybatis",  # MySQL 用户名
        password="mybatis",  # MySQL 密码
        database="mybatis"  # 需要连接的数据库
    )

    mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024)

    ret = mb.select_one("testBasic1", {'id':1})

    print(ret)

if __name__ == "__main__":
    main()
```

## Dynamic SQL
### ${}和#{}的区别
#{}是一个占位符，为prepared statement而存在，在MapperManager处理后会变成字符'?'；
${}表示简单的字符串替换。下面一个例子能说明它的区别：
```python
from mybatis import *

mm = MapperManager()

'''
test.xml的内容如下：

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
结果是
```bash
SELECT * from fruits_20241204 where id=? [1]
```
可以看见${date}被替换成"20241204"，而#{id}替换成了'?'，同时param_list中只有一个参数值为1。

基于安全性的考虑，为了防止SQL注入，建议只要能使用#{}就不要使用${}，除非你有足够的把握。

## Cache
mybatis-py为每条连接维护一个缓存池，淘汰策略是LRU，你可以定义池最大的字节容量，如果你不想使用cache，可以设置参数配置，代码如下：
```python
def main():
    # 连接到 MySQL 数据库
    # conn = mysql.connector.connect(
    #     host="localhost",  # MySQL 主机地址
    #     user="mybatis",  # MySQL 用户名
    #     password="mybatis",  # MySQL 密码
    #     database="mybatis"  # 需要连接的数据库
    # )

    mb = Mybatis(conn, "mapper", cache_memory_limit=50*1024*1024) # 容量上限为50MB
    mb2 = Mybatis(conn, "mapper", cache_memory_limit=None) # 不开启缓存
```