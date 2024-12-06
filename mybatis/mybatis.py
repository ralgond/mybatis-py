from typing import Optional, Dict, List

from .mapper_manager import MapperManager
from .cache import Cache, CacheKey

import os

class Mybatis(object):
    def __init__(self, conn, mapper_path:str, cache_memory_limit:Optional[int]=None, cache_max_live_ms:int=5*1000):
        self.conn = conn
        self.mapper_manager = MapperManager()

        if cache_memory_limit is not None:
            self.cache = Cache(cache_memory_limit, cache_max_live_ms)
        else:
            self.cache = Cache(0, cache_max_live_ms)

        mapper_file_name_l = [name for name in os.listdir(mapper_path) if name.endswith(".xml")]
        for file_name in mapper_file_name_l:
            full_path = os.path.join(mapper_path, file_name)
            self.mapper_manager.read_mapper_xml_file(full_path)

    def select_one(self, id:str, params:dict) -> Optional[Dict]:
        sql, param_list = self.mapper_manager.select(id, params)
        if self.cache is not None:
            res = self.cache.get(CacheKey(sql, param_list))
            if res is not None:
                return res

        with self.conn.cursor(prepared=True) as cursor:
            cursor.execute(sql, param_list)
            ret = cursor.fetchone()
            if ret is None:
                return None
            column_name = [item[0] for item in cursor.description]
            res = {}
            for idx, item in enumerate(column_name):
                res[item] = ret[idx]

            if self.cache is not None:
                self.cache.put(CacheKey(sql, param_list), res)
            return res

    def select_many(self, id:str, params:dict) -> Optional[List[Dict]]:
        sql, param_list = self.mapper_manager.select(id, params)
        if self.cache is not None:
            res = self.cache.get(CacheKey(sql, param_list))
            if res is not None:
                return res

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

            if len(res_list) == 0:
                res_list = None

            if self.cache is not None:
                self.cache.put(CacheKey(sql, param_list), res_list)

            return res_list

    def update(self, id:str, params:dict) -> int:
        '''
        :param id: mapper id
        :param params:
        :return: affected rows
        '''
        sql, param_list = self.mapper_manager.update(id, params)

        res = self.cache.clear()

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

        res = self.cache.clear()

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

        res = self.cache.clear()

        with self.conn.cursor(prepared=True) as cursor:
            cursor.execute(sql, param_list)
            self.conn.commit()
            last_id = cursor.lastrowid
            return last_id


    def SelectOne(self, unparsed_sql:str) -> Optional[Dict]:
        def decorator(func):
            def wrapper(*args, **kwargs):
                params = {}
                for key, value in kwargs.items():
                    params[key] = value

                sql, param_list = self.mapper_manager._to_prepared_statement(unparsed_sql, params)
                sql = self.mapper_manager._to_replace(sql, params)

                if self.cache is not None:
                    res = self.cache.get(CacheKey(sql, param_list))
                    if res is not None:
                        return res

                with self.conn.cursor(prepared=True) as cursor:
                    cursor.execute(sql, param_list)
                    ret = cursor.fetchone()
                    if ret is None:
                        return None

                    column_name = [item[0] for item in cursor.description]
                    res = {}
                    for idx, item in enumerate(column_name):
                        res[item] = ret[idx]

                    if self.cache is not None:
                        self.cache.put(CacheKey(sql, param_list), res)

                    return res

            return wrapper
        return decorator

    def SelectMany(self, unparsed_sql:str) -> Optional[List[Dict]]:
        def decorator(func):
            def wrapper(*args, **kwargs):
                params = {}
                for key, value in kwargs.items():
                    params[key] = value

                sql, param_list = self.mapper_manager._to_prepared_statement(unparsed_sql, params)
                sql = self.mapper_manager._to_replace(sql, params)

                if self.cache is not None:
                    res = self.cache.get(CacheKey(sql, param_list))
                    if res is not None:
                        return res

                with self.conn.cursor(prepared=True) as cursor:
                    cursor.execute(sql, param_list)

                    res_list = []
                    column_name = [item[0] for item in cursor.description]

                    ret = cursor.fetchall()
                    for row in ret:
                        d = {}
                        for idx, item in enumerate(column_name):
                            d[item] = row[idx]
                        res_list.append(d)

                    if len(res_list) == 0:
                        res_list = None

                    if self.cache is not None:
                        self.cache.put(CacheKey(sql, param_list), res_list)

                    return res_list
            return wrapper
        return decorator

    def Insert(self, unparsed_sql:str) -> int:
        def decorator(func):
            def wrapper(*args, **kwargs):
                params = {}
                for key, value in kwargs.items():
                    params[key] = value

                sql, param_list = self.mapper_manager._to_prepared_statement(unparsed_sql, params)
                sql = self.mapper_manager._to_replace(sql, params)

                res = self.cache.clear()

                with self.conn.cursor(prepared=True) as cursor:
                    cursor.execute(sql, param_list)
                    self.conn.commit()
                    last_id = cursor.lastrowid
                    return last_id
            return wrapper
        return decorator

    def Delete(self, unparsed_sql:str) -> int:
        def decorator(func):
            def wrapper(*args, **kwargs):
                params = {}
                for key, value in kwargs.items():
                    params[key] = value

                sql, param_list = self.mapper_manager._to_prepared_statement(unparsed_sql, params)
                sql = self.mapper_manager._to_replace(sql, params)

                res = self.cache.clear()

                with self.conn.cursor(prepared=True) as cursor:
                    cursor.execute(sql, param_list)
                    affected_rows = cursor.rowcount
                    self.conn.commit()
                    return affected_rows
            return wrapper
        return decorator

    def Update(self, unparsed_sql:str) -> int:
        def decorator(func):
            def wrapper(*args, **kwargs):
                params = {}
                for key, value in kwargs.items():
                    params[key] = value

                sql, param_list = self.mapper_manager._to_prepared_statement(unparsed_sql, params)
                sql = self.mapper_manager._to_replace(sql, params)

                res = self.cache.clear()

                with self.conn.cursor(prepared=True) as cursor:
                    cursor.execute(sql, param_list)
                    affected_rows = cursor.rowcount
                    self.conn.commit()
                    return affected_rows

            return wrapper

        return decorator