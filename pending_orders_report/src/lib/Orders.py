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
from openpyxl.styles import Font, Color, colors

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))


class Orders(object):
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
            message.attach(MIMEText("Dear Team,\nAttached is today's High Value Orders report."))
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

    def pending_orders_report(self):
        '''
        Get the orders that have delivery date greater than romorrow
        '''
        order = None
        # del_date = datetime.today().date() + timedelta(days=1)
        del_date = datetime.strptime("2018-03-20", "%Y-%m-%d").date()
        # del_date = datetime.today().date()
        try:
            sql = '''select pt.default_code, pt.name, pc2.name, pc.name, sum(sol.product_uom_qty) 
                     from sale_order so 
                     left join sale_order_line sol ON sol.order_id = so.id 
                     left join product_product pp ON pp.id = sol.product_id 
                     left join product_template pt ON pp.product_tmpl_id = pt.id 
                     left join product_category pc ON pc.id = pt.categ_id 
                     left join product_category pc2 on pc2.id = pc.parent_id 
                     where so.state='progress' and so.date_delivery > %s 
                     group by pt.default_code, pt.name, pc2.name, pc.name;'''
            params = (str(del_date), )
            order = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return order

    def format_xlsx(self, orders):
        '''
        Turn the received dictionary into an xlsx file to be sent out
        '''
        result_file = Workbook()

        active_sheet = result_file.active
        headers = ['CODE', 'PRODUCT NAME', 'CATEGORY',
                   'SUB-CATEGORY', 'TOTAL QUANTITY PENDING ORDERS']
        empty_field = ['', '', '', '', '', '']
        active_sheet.append(headers)
	#active_sheet.append(empty_field)
        for order in orders:
            my_order = ["'" + str(item)
                        if isinstance(item, datetime) == True
                        else item
                        for item in order]
            active_sheet.append(my_order)
        result_file.save('pending_orders_report.xlsx')


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
