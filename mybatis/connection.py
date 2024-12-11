import re
import sqlite3
import time
from abc import ABC, abstractmethod
from typing import Optional, Sequence

import psycopg2
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
import mysql.connector
from psycopg2.extensions import connection as PostgreSQLConnectionRaw
from psycopg2.extensions import cursor as PostgreSQLCursorRaw

from .errors import DatabaseError

from sqlite3 import Connection as Sqlite3ConnectionRaw
from sqlite3 import Cursor as Sqlite3CursorRaw



class AbstractCursor(ABC):
    @abstractmethod
    def execute(self, query: str, param_list : Sequence = None):
        pass # pragma: no cover

    @abstractmethod
    def rowcount(self):
        pass # pragma: no cover

    @abstractmethod
    def lastrowid(self):
        pass # pragma: no cover

    @abstractmethod
    def description(self):
        pass # pragma: no cover

    @abstractmethod
    def fetchone(self):
        pass # pragma: no cover

    @abstractmethod
    def fetchall(self):
        pass # pragma: no cover

    @abstractmethod
    def fetchmany(self, size: int):
        pass # pragma: no cover

    @abstractmethod
    def close(self):
        pass # pragma: no cover

class AbstractConnection(ABC):
    @abstractmethod
    def cursor(self, *args, **kwargs) -> AbstractCursor:
        pass # pragma: no cover

    @abstractmethod
    def close(self):
        pass # pragma: no cover

    @abstractmethod
    def set_autocommit(self, autocommit: bool):
        pass # pragma: no cover

    @abstractmethod
    def start_transaction(self):
        pass # pragma: no cover

    @abstractmethod
    def commit(self):
        pass # pragma: no cover

    @abstractmethod
    def rollback(self):
        pass # pragma: no cover

    @abstractmethod
    def need_returning_id(self):
        pass # pragma: no cover

    @abstractmethod
    def reconnect(self, attempts, delay):
        pass # pragma: no cover


class MySQLCursor(AbstractCursor):
    def __init__(self, cursor: MySQLCursorAbstract, *args, **kwargs):
        self.cursor = cursor

    def execute(self, query: str, param_list : Sequence = None):
        try:
            return self.cursor.execute(query, param_list)
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def rowcount(self):
        return self.cursor.rowcount

    def lastrowid(self):
        return self.cursor.lastrowid

    def description(self):
        return self.cursor.description

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def fetchmany(self, size: int):
        try:
            return self.cursor.fetchmany(size)
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def close(self):
        try:
            self.cursor.close()
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if exc_type:
            print(f"An exception occurred: {exc_val}")
        return False


class MySQLConnection(AbstractConnection):
    def __init__(self, conn: MySQLConnectionAbstract):
        self.conn = conn

    def cursor(self, *args, **kwargs) -> AbstractCursor:
        try:
            return MySQLCursor(cursor=self.conn.cursor(*args, **kwargs))
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def close(self):
        self.conn.close()

    def set_autocommit(self, autocommit: bool):
        self.conn.autocommit = autocommit

    def start_transaction(self):
        self.set_autocommit(False)

    def commit(self):
        try:
            self.conn.commit()
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def rollback(self):
        try:
            self.conn.rollback()
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

    def need_returning_id(self):
        return False

    def reconnect(self, attempts, delay):
        try:
            self.conn.reconnect(attempts, delay)
        except mysql.connector.Error as err:
            raise DatabaseError(str(err))

class PostgreSQLCursor(AbstractCursor):
    def __init__(self, cursor: PostgreSQLCursorRaw, prepared=False):
        self.prepared = prepared
        self.cursor = cursor
        self.replace_pattern = re.compile(r"\?")

    def execute(self, query: str, param_list:Sequence = None):
        query = self.replace_pattern.sub("%s", query)
        try:
            return self.cursor.execute(query, param_list)
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def rowcount(self):
        return self.cursor.rowcount

    def lastrowid(self):
        ret = None
        try:
            ret = self.cursor.fetchone()
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))
        return ret[0]

    def description(self):
        return self.cursor.description

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def fetchmany(self, size: int):
        try:
            return self.cursor.fetchmany(size)
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def close(self):
        try:
            self.cursor.close()
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if exc_type:
            print(f"An exception occurred: {exc_val}")
        return False


class PostgreSQLConnection(AbstractConnection):
    def __init__(self, conn: PostgreSQLConnectionRaw):
        self.conn = conn
        # self.prepared = False

    def cursor(self, *args, **kwargs) -> AbstractCursor:
        prepared = False
        if 'prepared' in kwargs:
            prepared = kwargs['prepared']
            del kwargs['prepared']
        try:
            return PostgreSQLCursor(cursor=self.conn.cursor(*args, **kwargs), prepared=prepared)
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def close(self):
        self.conn.close()

    def set_autocommit(self, autocommit: bool):
        self.conn.autocommit = autocommit

    def start_transaction(self):
        pass

    def commit(self):
        try:
            self.conn.commit()
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def rollback(self):
        try:
            self.conn.rollback()
        except psycopg2.errors.Error as err:
            raise DatabaseError(str(err))

    def need_returning_id(self):
        return True

    def reconnect(self, attempts, delay):
        for i in range(attempts):
            try:
                if self.conn.closed:
                    self.conn = psycopg2.connect(**self.connect_kwargs)
                if self.conn:
                    break
            except psycopg2.errors.OperationalError as err:
                time.sleep(delay)
        if self.conn is not None and self.conn.closed is False:
            return
        else:
            raise DatabaseError(str("Reconnecting failed."))

class Sqlite3Cursor(AbstractCursor):
    def __init__(self, cursor: Sqlite3CursorRaw, *args, **kwargs):
        self.cursor = cursor

    def execute(self, query: str, param_list: Sequence = None):
        try:
            if param_list is None:
                return self.cursor.execute(query)
            else:
                return self.cursor.execute(query, param_list)
        except sqlite3.Error as err:
            raise DatabaseError(str(err))

    def rowcount(self):
        return self.cursor.rowcount

    def lastrowid(self):
        return self.cursor.lastrowid

    def description(self):
        return self.cursor.description

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        except sqlite3.Error as err:
            raise DatabaseError(str(err))

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        except sqlite3.Error as err:
            raise DatabaseError(str(err))

    def fetchmany(self, size: int):
        try:
            return self.cursor.fetchmany(size)
        except sqlite3.Error as err:
            raise DatabaseError(str(err))

    def close(self):
        try:
            self.cursor.close()
        except sqlite3.Error as err:
            raise DatabaseError(str(err))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if exc_type:
            print(f"An exception occurred: {exc_val}")
        return False

class Sqlite3Connection(AbstractConnection):
    def __init__(self, conn: Sqlite3ConnectionRaw):
        self.conn = conn

    def cursor(self, *args, **kwargs) -> AbstractCursor:
        prepared = False
        if 'prepared' in kwargs:
            prepared = kwargs['prepared']
            del kwargs['prepared']
        return Sqlite3Cursor(cursor=self.conn.cursor(*args, **kwargs))

    def close(self):
        self.conn.close()

    def set_autocommit(self, autocommit: bool):
        pass

    def start_transaction(self):
        pass

    def commit(self):
        try:
            self.conn.commit()
        except sqlite3.Error as err:
            raise DatabaseError(str(err))

    def rollback(self):
        try:
            self.conn.rollback()
        except sqlite3.Error as err:
            raise DatabaseError(str(err))

    def need_returning_id(self):
        return False

    def reconnect(self, attempts, delay):
        pass

class ConnectionFactory(ABC):
    @staticmethod
    def get_connection(*args, **kwargs) -> Optional[AbstractConnection]:
        dbms_name = kwargs.get("dbms_name")
        del kwargs['dbms_name']
        if dbms_name == 'mysql':
            conn = mysql.connector.connect(
                **kwargs
            )
            return MySQLConnection(conn)
        elif dbms_name == 'postgresql':
            conn = psycopg2.connect(
                **kwargs
            )
            ret_conn = PostgreSQLConnection(conn)
            ret_conn.__setattr__("connect_kwargs", kwargs)
            return ret_conn
        elif dbms_name == 'sqlite3':
            db_path = kwargs.get("db_path")
            conn = sqlite3.connect(db_path)
            ret_conn = Sqlite3Connection(conn)
            return ret_conn
        else:
            raise NotImplementedError(dbms_name)