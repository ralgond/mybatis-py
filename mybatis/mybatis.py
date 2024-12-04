from typing import Optional, Dict, List

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

    def select_one(self, id:str, params:dict) -> Optional[Dict]:
        sql, param_list = self.mapper_manager.select(id, params)
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

    def select_many(self, id:str, params:dict) -> Optional[List[Dict]]:
        sql, param_list = self.mapper_manager.select(id, params)
        with self.conn.cursor(prepared=True) as cursor:
            cursor.execute(sql, param_list)
            ret = cursor.fetchall()
            if ret is None:
                return None
            column_name = [item[0] for item in cursor.description]
            res_list = []
            for row in ret:
                d = {}
                for idx, item in enumerate(column_name):
                    d[item] = row[idx]
                res_list.append(d)
            return res_list

    def update(self, id:str, params:dict) -> int:
        '''
        :param id: mapper id
        :param params:
        :return: affected rows
        '''
        sql, param_list = self.mapper_manager.update(id, params)
        with self.conn.cursor(prepared=True) as cursor:
            cursor.execute(sql, param_list)
            affected_rows = cursor.rowcount
            self.conn.commit()
            return affected_rows

    def delete(self, id:str, params:dict) -> int:
        '''
        :param id: mapper id
        :param params:
        :return: affected rows
        '''
        sql, param_list = self.mapper_manager.delete(id, params)
        with self.conn.cursor(prepared=True) as cursor:
            cursor.execute(sql, param_list)
            affected_rows = cursor.rowcount
            self.conn.commit()
            return affected_rows

    def insert(self, id:str, params:dict) -> int:
        '''
        :param id: mapper id
        :param params:
        :return: last auto incremented row id
        '''
        sql, param_list = self.mapper_manager.insert(id, params)
        with self.conn.cursor(prepared=True) as cursor:
            cursor.execute(sql, param_list)
            self.conn.commit()
            last_id = cursor.lastrowid
            return last_id
