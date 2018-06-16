"""Lazy initialization design for Python psycopg2"""
import psycopg2
import psycopg2.extras


# pylint: disable-msg=R0903
class Postgres(object):
    """Lazy DB proxy."""
    # __shared_state = {}
    # pylint: disable-msg=R0913
    def __init__(self, host, port, user, passwd, database, connection_timeout):
        # self.__dict__ = self.__shared_state
        self.__connection = None
        self.__host = host
        self.__port = port
        self.__user = user
        self.__passwd = passwd
        self.__db = database
        self.__connection_timeout = connection_timeout

    def __getattr__(self, name):
        if self.__connection is None:
            try:
                self.__connection = psycopg2.connect(host=self.__host,
                                                     port=self.__port,
                                                     user=self.__user,
                                                     password=self.__passwd,
                                                     database=self.__db,
                                                     connect_timeout=self.__connection_timeout,
                                                     cursor_factory=psycopg2.extras.DictCursor)
            except psycopg2.Error:
                raise
        return getattr(self.__connection, name)

    def close(self):
        """Close the connection"""
        if self.__connection is not None:
            self.__connection.close()
