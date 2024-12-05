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

def test_foreach():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")

    sql, param_list = mm.select("testForeach", {'names': [1, 2, 3, 4]})
    assert sql == "SELECT name, category, price FROM fruits WHERE category = 'apple' AND name IN ( ? , ? , ? , ? )"
    assert len(param_list) == 4
    assert param_list[0] == 1
    assert param_list[1] == 2
    assert param_list[2] == 3
    assert param_list[3] == 4

def test_trim():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")

    sql, param_list = mm.select("testTrim", {'names': [1, 2, 3, 4]})
    assert sql == "SELECT name, category, price FROM fruits WHERE category = 'apple' OR price = 200 AND (type = 1 OR type= 0)"

def test_trim2():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")

    sql, param_list = mm.insert("testInsertSelective", {'name': 'Candy', 'category': "C", 'price': 500})
    assert sql == "insert into fruits ( name, category, price ) values ( ?, ?, ? )"
    assert len(param_list) == 3
    assert param_list[0] == "Candy"
    assert param_list[1] == 'C'
    assert param_list[2] == 500

def test_set():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")

    sql, param_list = mm.update("testSet", {'category': "banana", "price": 500, "name": "a"})
    assert sql == "UPDATE fruits SET category = ?, price = ? WHERE name = ?"
    assert len(param_list) == 3
    assert param_list[0] == "banana"
    assert param_list[1] == 500
    assert param_list[2] == "a"

def test_dollar():
    mm = MapperManager()
    mm.read_mapper_xml_file("mapper/test.xml")

    sql, param_list = mm.select("testStringReplace", {'id': 1, 'date': "20241204"})
    assert sql == "SELECT * from fruits_20241204 where id=?"
    assert len(param_list) == 1
    assert param_list[0] == 1