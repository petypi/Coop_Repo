'''
Main library for processing the Copia Institutions Data
'''
import psycopg2
from datetime import datetime, timedelta
import csv
from openpyxl import Workbook
from Postgres import Postgres
from email.mime.multipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email import Encoders
import ast
import smtplib
import ConfigParser
import os


CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))

class Institutions(object):
    '''
    Fetch current balances from the database and mail them to concerned party
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
            toaddr = ast.literal_eval(EMAIL_SETTINGS['recipients'])
            message['Subject'] = EMAIL_SETTINGS['subject']
            message.attach(MIMEText("Dear Team,\nAttached is today's report."))
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
            server.sendmail(EMAIL_SETTINGS['username'], toaddr, message.as_string())
            server.close()
        except smtplib.SMTPException:
            raise

    def get_institution_deliveries(self, delivery_date):
        '''
        Get expected deliveries for the day after current date
        '''
        deliveries = None
        try:
            """
            sql = '''select rp.name institution, count(so.id) orders, 
                     date(so.create_date) date_created, so.date_delivery 
                     from sale_order so, res_partner rp, 
                     res_partner_res_partner_category_rel rppcr  
                     where so.vendor_partner_id = rp.id and 
                     so.date_delivery = %s and rp.agent = true 
                     and rppcr.partner_id = so.vendor_partner_id and 
                     rppcr.category_id = 5 group by so.vendor_partner_id, 
                     rp.name, so.date_delivery, date(so.create_date);'''
            """
            sql = '''
                    SELECT
                        rp.name AS institution, COUNT(so.id) AS orders,
                        DATE(so.create_date) AS date_created,
                        so.date_delivery
                    FROM sale_order so
                    LEFT JOIN res_partner rp
                        ON so.partner_id = rp.id
                    WHERE so.date_delivery = %s
                    AND rp.is_agent = true
                    AND rp.name LIKE '%%-%%'
                    GROUP BY so.partner_id, rp.name, so.date_delivery, DATE(so.create_date);
                    '''
            params = (str(delivery_date), )
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
        headers = ['Name', 'Number of Orders', 'Date Created', 'Delivery Date']
        active_sheet.append(headers)
        for delivery in deliveries:
            active_sheet.append(delivery)
        result_file.save('expected_deliveries.xlsx')


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

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    BALANCER = Institutions(CONN)
    delivery_date1 = datetime.today() + timedelta(days=1)
    delivery_date1 = '2016-02-04'
    #delivery_date2 = datetime.today() + timedelta(days=2)
    delivery_date2 = '2016-02-05'
    deliveries = BALANCER.get_institution_deliveries(delivery_date1)
    deliveries2 = BALANCER.get_institution_deliveries(delivery_date2)
    null_return = [deliveries.append(x) for x in deliveries2]
    BALANCER.format_xlsx(deliveries)
    BALANCER.email_results('expected_deliveries.xlsx')
