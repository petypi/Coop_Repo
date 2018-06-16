# DB

import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from lib.utility import Utility

utility = Utility()

class DB(object):

    # SqlAlchemy
    _url = "postgresql://{}:{}@{}:{}/{}"
    pull_url = _url.format(
        utility._config_parser.get("pull", "username"), 
        utility._config_parser.get("pull", "password"),
        utility._config_parser.get("pull", "host"),
        utility._config_parser.get("pull", "port"),
        utility._config_parser.get("pull", "database")
    )

    push_url = _url.format(
        utility._config_parser.get("push", "username"),
        utility._config_parser.get("push", "password"),
        utility._config_parser.get("push", "host"),
        utility._config_parser.get("push", "port"),
        utility._config_parser.get("push", "database")
    )

    pull_conn = sqlalchemy.create_engine(pull_url, client_encoding="utf8")
    pull_meta = sqlalchemy.MetaData(bind=pull_conn, reflect=False)

    push_conn = sqlalchemy.create_engine(push_url, client_encoding="utf8")
    push_meta = sqlalchemy.MetaData(bind=push_conn, reflect=False)

    # Session
    push_session = sessionmaker()
    push_session.configure(bind=push_conn)
