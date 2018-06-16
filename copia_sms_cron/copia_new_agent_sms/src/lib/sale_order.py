"""
Main library for sending new agent sms via xmlrpc
"""

import ConfigParser
import xmlrpclib
import os
from datetime import datetime, timedelta

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
ERP_CREDENTIALS = dict(CONFIG_READER.items('erplogin'))


class sale_order(object):

    def send_agent_sms(self):
        '''
        send new agent sms
        '''
        server_url = ERP_CREDENTIALS['server_url']
        server_port = ERP_CREDENTIALS['server_port']
        server_db = ERP_CREDENTIALS['server_db']
        username = ERP_CREDENTIALS['username']
        password = ERP_CREDENTIALS['passwrd']       
        local_url = 'http://%s:%d/xmlrpc/2/common' % (server_url,
                                                      int(server_port))
        common = xmlrpclib.ServerProxy(local_url)
        
        uid = common.login(server_db, username, password)
        
        object_url = 'http://%s:%d/xmlrpc/2/object' % (server_url,
                                                      int(server_port))
        sock = xmlrpclib.ServerProxy(object_url)
        result = sock.execute(
            server_db, uid, password, 'res.partner', 'action_sms_new_agent', [2]
        )
        return result