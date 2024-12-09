import re
from abc import ABC, abstractmethod
from typing import Optional, Sequence

import psycopg2
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
import mysql.connector
from psycopg2.extensions import connection as PostgreSQLConnectionRaw
from psycopg2.extensions import cursor as PostgreSQLCursorRaw

class AbstractCursor(ABC):
    @abstractmethod
    def execute(self, query: str, param_list : Sequence = None):
        pass

    @abstractmethod
    def rowcount(self):
        pass

    @abstractmethod
    def lastrowid(self):
        pass

    @abstractmethod
    def description(self):
        pass

    @abstractmethod
    def fetchone(self):
        pass

    @abstractmethod
    def fetchall(self):
        pass

    @abstractmethod
    def fetchmany(self, size: int):
        pass

    @abstractmethod
    def close(self):
        pass

class AbstractConnection(ABC):
    @abstractmethod
    def cursor(self, *args, **kwargs) -> AbstractCursor:
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def set_autocommit(self, autocommit: bool):
        pass

    @abstractmethod
    def start_transaction(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass


class MySQLCursor(AbstractCursor):
    def __init__(self, cursor: MySQLCursorAbstract, *args, **kwargs):
        self.cursor = cursor

    def execute(self, query: str, param_list : Sequence = None):
        return self.cursor.execute(query, param_list)

    def rowcount(self):
        return self.cursor.rowcount

    def lastrowid(self):
        return self.cursor.lastrowid

    def description(self):
        return self.cursor.description

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchmany(self, size: int):
        return self.cursor.fetchmany(size)

    def close(self):
        self.cursor.close()

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
        return MySQLCursor(cursor=self.conn.cursor(*args, **kwargs))

    def close(self):
        self.conn.close()

    def set_autocommit(self, autocommit: bool):
        self.conn.autocommit = autocommit

    def start_transaction(self):
        self.set_autocommit(False)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

class PostgreSQLCursor(AbstractCursor):
    def __init__(self, cursor: PostgreSQLCursorRaw, prepared=False):
        self.prepared = prepared
        self.cursor = cursor
        self.replace_pattern = re.compile(r"\?")

    def execute(self, query: str, param_list:Sequence = None):
        query = self.replace_pattern.sub("%s", query)
        return self.cursor.execute(query, param_list)

    def rowcount(self):
        return self.cursor.rowcount

    def lastrowid(self):
        return self.cursor.fetchone()[0]

    def description(self):
        return self.cursor.description

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchmany(self, size: int):
        return self.cursor.fetchmany(size)

    def close(self):
        self.cursor.close()

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
        self.prepared = False

    def cursor(self, *args, **kwargs) -> AbstractCursor:
        prepared = False
        if 'prepared' in kwargs:
            prepared = kwargs['prepared']
            del kwargs['prepared']
        return PostgreSQLCursor(cursor=self.conn.cursor(*args, **kwargs), prepared=prepared)

    def close(self):
        self.conn.close()

    def set_autocommit(self, autocommit: bool):
        self.conn.autocommit = autocommit

    def start_transaction(self):
        pass

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()


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
            return PostgreSQLConnection(conn)