from typing import Optional

import mysql.connector
import sqlite3

from .mapper_manager import MapperManager
import os

class Mybatis(object):
    def __init__(self, conn, mapper_path:str):
        self.conn = conn
        self.mapper_manager = MapperManager()

        mapper_file_name_l = [name for name in os.listdir(mapper_path) if name.endswith(".xml")]
        for file_name in mapper_file_name_l:
            full_path = os.path.join(mapper_path, file_name)
            self.mapper_manager.read_mapper_xml_file(full_path)

    def select_one(self, id:str, param:dict) -> Optional[dict]:
        sql, param_list = self.mapper_manager.select(id, param)
        with self.conn.cursor(prepared=True) as cursor:
            cursor.execute(sql, param_list)
            ret = cursor.fetchone()
            if ret is None:
                return None
            column_name = [item[0] for item in cursor.description]
            res = {}
            for idx, item in enumerate(column_name):
                res[item] = ret[idx]
            return res
