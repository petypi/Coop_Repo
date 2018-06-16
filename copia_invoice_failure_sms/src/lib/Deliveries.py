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
from openpyxl import Workbook
from AfricasTalkingGateway import AfricasTalkingGateway, AfricasTalkingGatewayException

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
SMS_SETTINGS = dict(CONFIG_READER.items('sms'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))


class Deliveries(object):
    '''
    Fetch newest product changes from the database and mail them to concerned
    party
    '''
    def __init__(self, db_connection):
        self.db_management = DatabaseManagement(db_connection)

    def email_results(self, filename):
        '''
        Send the files to the team involved
        '''
        try:
            message = MIMEMultipart('alternative')
            message['From'] = "Copia Report Automation"
            toaddr = ast.literal_eval(EMAIL_SETTINGS['recepients'])
            message['Subject'] = EMAIL_SETTINGS['subject']
            message.attach(MIMEText("Dear Team,\nThe deliveries in the attached document have not been processed/invoiced. Manual intervention is required."))
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(file(filename).read())
            Encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition',
                                  'attachment',
                                  filename=filename)
            message.attach(attachment)
            password = EMAIL_SETTINGS['password']
            server = smtplib.SMTP(EMAIL_SETTINGS['server'],
                                  int(EMAIL_SETTINGS['port']))
            server.ehlo()
            server.starttls()
            server.login(EMAIL_SETTINGS['username'], password)
            server.sendmail(EMAIL_SETTINGS['username'], toaddr,
                            message.as_string())
            server.close()
        except smtplib.SMTPException:
            raise
            
    def sms_results(self, number):
        '''
        Send an sms to notify of failure
        '''
        username = SMS_SETTINGS['username']
        apikey = SMS_SETTINGS['apikey']
        sender = SMS_SETTINGS['sender']

        #sender = '40707'
        msisdn = ast.literal_eval(SMS_SETTINGS['recipients'])
        message = "Hi. ERP invoicing might have experienced issues and %s deliveries have not been processed. Manual intervention is required."%number
        gateway = AfricasTalkingGateway(username, apikey)
        msisdn = '+254721458132'
        to = SMS_SETTINGS['recipients']
        print "Username: %s Sender: %s Key: %s"%(username, sender, apikey)
        try:
            results = gateway.sendMessage(str(to), str(message), str(sender))
            for recipient in results:
                print 'number=%s;status=%s;messageId=%s;cost=%s' % (recipient['number'],
                                                            recipient['status'],
                                                            recipient['messageId'],
                                                            recipient['cost'])
        except AfricasTalkingGatewayException, e:
            print 'Encountered an error while sending: %s' % str(e)

    def get_unsynced_deliveries(self):
        '''
        Get unsynced deliveries
        '''
        deliveries = None
        today = datetime.today().date() # + timedelta(days=1)
        try:
            sql = '''
                    SELECT
                        so.name, sol.name, od.qty, od.balance, od.receipt_ref,
                        od.date_sync, od.date_delivery
                    FROM ofs_delivery od
                    JOIN sale_order_line sol
                        ON sol.id = od.line_id
                    JOIN sale_order so
                        ON so.id = sol.order_id
                    WHERE od.processed = FALSE
                    '''
            params = ()
            deliveries = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return deliveries

    def format_xlsx(self, deliveries):
        '''
        Turn the received dictionary into an xlsx file to be sent out
        '''
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Order Number', 'Item', 'Delivered Qty', 'Balance',
                   'Receipt Number', 'Date Synced', 'Forecasted Delivery Date']
        active_sheet.append(headers)
        for delivery in deliveries:
            delivery = ["'" + str(item)
                        if isinstance(item, datetime) == True
                        else item
                        for item in delivery]
            active_sheet.append(delivery)
        result_file.save('unprocessed_deliveries.xlsx')


class DatabaseManagement(object):
    '''
    The basic class for DB management methods
    '''
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def retrieve_all_data_params(self, sql, params):
        '''
        Retrieve all data associated with the params from the database
        '''
        results = None
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql, params)
            results = cursor.fetchall()
        except psycopg2.Error:
            raise
        return results

    def execute_query(self, sql, params):
        '''
        Run a query that does not require a return
        '''
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql, params)
            self.db_connection.commit()
        except psycopg2.DatabaseError:
            raise
