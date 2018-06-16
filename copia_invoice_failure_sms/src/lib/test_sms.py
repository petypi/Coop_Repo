'''
Main library for processing the deliveries intended for new agents
'''
import psycopg2
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders as Encoders
import ast
import smtplib
import ConfigParser
import os
from AfricasTalkingGateway import AfricasTalkingGateway, AfricasTalkingGatewayException

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
SMS_SETTINGS = dict(CONFIG_READER.items('sms'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))
 
def sms_results(number):
    '''
    Send an sms to notify of failure
    '''
    username = SMS_SETTINGS['username']
    apikey = SMS_SETTINGS['apikey']
    sender = SMS_SETTINGS['sender']

    #sender = '40707'
    msisdn = ast.literal_eval(SMS_SETTINGS['recipients'])
    message = "Hi. Test sms: %s lines have not been uploaded. Manual intervention is required."%number
    gateway = AfricasTalkingGateway(username, apikey)
    to = ast.literal_eval(SMS_SETTINGS['recipients'])
    print "Username: %s Sender: %s Key: %s To: %s"%(username, sender, apikey, to)
    try:
        results = gateway.sendMessage(str(to), str(message), str(sender))
        for recipient in results:
            print 'number=%s;status=%s;messageId=%s;cost=%s' % (recipient['number'],
                                                        recipient['status'],
                                                        recipient['messageId'],
                                                        recipient['cost'])
    except AfricasTalkingGatewayException, e:
        print 'Encountered an error while sending: %s' % str(e)
        
sms_results(100)