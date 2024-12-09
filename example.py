from mybatis import *

def main():
    mm = MapperManager()

    mm.read_mapper_xml_file("mapper/test_returning_id.xml")

    # sql, param_list = mm.select("testInclude", {'category':"A", "price":500})
    # print(sql, param_list)

    # sql, param_list = mm.select("testIf", {'category':"A", "price":500})
    # print(sql, param_list)

    # sql, param_list = mm.select("testWhere", {'category': "A", "price": 500})
    # print(sql, param_list)

    # sql, param_list = mm.select("testChoose", {'category': "banana", "price": 500})
    # print(sql, param_list)

    # sql, param_list = mm.select("testForeach", {'names':[1,2,3,4]})
    # print(sql, param_list)

    # sql, param_list = mm.select("testTrim", {'names': [1, 2, 3, 4]})
    # print(sql, param_list)

    # sql, param_list = mm.update("testSet", {'category': "banana", "price": 500, "name":"a"})
    # print(sql, param_list)

    # sql, param_list = mm.select("testStringReplace", {'id':1, 'date':"20241204"})
    # print(sql, param_list)

    # sql, param_list = mm.insert("testInsertSelective", {'name': 'Candy', 'category': "C", 'price':500})
    # print(sql, param_list)

    sql, param_list = mm.insert("test_returning_id.insert", {'name': 'Candy', 'category': "C", 'price':500, '__need_returning_id__':'fid'})
    print(sql, param_list)

    # cur.execute(sql, param_list, multi=True)
    #
    # print(cur.description)
    #
    # res = cur.fetchall()
    # print(res)

    # print(globals()['A'])

if __name__ == "__main__":
    main()