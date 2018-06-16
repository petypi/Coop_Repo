#! /usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from psycopg2 import OperationalError, InternalError
from lib.utility import Utility

utility = Utility()


class Database:

    def __init__(self):

        self.connection = None
        self.username = utility._config_parser.get("pull", "username")
        self.password = utility._config_parser.get("pull", "password")
        self.host = utility._config_parser.get("pull", "host")
        self.port = utility._config_parser.get("pull", "port")
        self.database = utility._config_parser.get("pull", "database")
        self.__cursor = None

    @property
    def cursor(self):
        if self.is_connected():
            return self.__cursor
        raise OperationalError('Database connection failed')

    def is_connected(self):
        print('db: {dbname}\nuser: {user}\nhost: {host}\npass: {password}\nport: {port}'.format(dbname=self.database,
                                                                                                user=self.username,
                                                                                                host=self.host,
                                                                                                password=self.password,
                                                                                                port=self.port))
        if self.connection is None:
            try:
                conn = psycopg2.connect(dbname=self.database,
                                        user=self.username,
                                        host=self.host,
                                        password=self.password,
                                        port=self.port)
            except OperationalError:
                return False
            else:
                self.connection = conn
                self.__cursor = self.connection.cursor()

        return True

    def execute(self, query, params=None):
        if params is None:
            try:
                self.cursor.execute(query)
            except InternalError:
                self.cursor.execute('ROLLBACK')
            finally:
                self.cursor.execute(query)
        else:
            try:
                self.cursor.execute(query, params)
            except InternalError:
                self.cursor.execute('ROLLBACK')
            finally:
                self.cursor.execute(query, params)

    def fetchall(self, query, params=None, with_col_names=False):
        self.execute(query, params)
        if with_col_names:
            colnames = [desc[0] for desc in self.cursor.description]
            return colnames, self.cursor.fetchall()

        return self.cursor.fetchall()

    def close(self):
        if self.is_connected():
            self.connection.close()
            self.connection = None
