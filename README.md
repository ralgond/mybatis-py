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
