import pytest
from mybatis import MapperManager

def test_include():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")
    sql, param_list = mm.select("testInclude", {'category': "A", "price": 500})
    assert sql == "SELECT name, category, price FROM fruits WHERE category = ?"
    assert len(param_list) == 1
    assert param_list[0] == "A"

def test_if():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")
    sql, param_list = mm.select("testIf", {'category': "A", "price": 500})
    assert sql == "SELECT name, category, price FROM fruits WHERE 1=1 AND category = ? AND price = ? AND name = 'pear' AND 1=1"
    assert len(param_list) == 2
    assert param_list[0] == "A"
    assert param_list[1] == 500

def test_where():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")

    sql, param_list = mm.select("testWhere", {'category': "A", "price": 500})
    assert sql == "SELECT name, category, price FROM fruits WHERE category = 'apple' AND price = ?"
    assert len(param_list) == 1
    assert param_list[0] == 500

def test_choose():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")

    sql, param_list = mm.select("testChoose", {'name':1, 'category': "banana2", "price": 500})
    assert sql == "SELECT name, category, price FROM fruits WHERE name = ?"
    assert len(param_list) == 1
    assert param_list[0] == 1

    sql, param_list = mm.select("testChoose", {'category': "banana2", "price": 500})
    assert sql == "SELECT name, category, price FROM fruits WHERE category = 'apple'"
    assert len(param_list) == 0

    sql, param_list = mm.select("testChoose", {'category': "banana", "price": 500})
    assert sql == "SELECT name, category, price FROM fruits WHERE category = ? AND price = ?"
    assert len(param_list) == 2
    assert param_list[0] == "banana"
    assert param_list[1] == 500
