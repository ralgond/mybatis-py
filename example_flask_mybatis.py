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
