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