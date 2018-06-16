'''
Main library for processing the orders for the today, for the workload in the warehouse
'''
import psycopg2
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders as Encoders
import ast
import smtplib
import ConfigParser
import os
from openpyxl import Workbook

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))


class OrdersToday(object):
    '''
    Orders that are to be delivered tomorrow (orders before 9:30 am.
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
            message.attach(MIMEText("Dear Team,\nAttached is today's Orders."))
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

    def get_todays_orders(self):
        '''
        Get the newest agents
        '''
        orders = None
        if date.weekday(datetime.today()) == 0:
            orderDate = datetime.today().date() - timedelta(days=2)
        else:
            orderDate = datetime.today().date() - timedelta(days=1)
        try:
            sql = '''
                    SELECT
                        so.name OrderNumber,so.date_order OrderDate,
                        so.date_delivery DeliveryDate,prd.default_code ItemNumber,
                        pt.name ItemName, sol.product_uom_qty QtyOrdered,
                        prt.name Agent, cust.name Customer,rpd.route_id Route,
                        (sol.price_unit * sol.product_uom_qty) SaleValue
                    FROM sale_order so
                    LEFT JOIN sale_order_line sol
                        ON so.id = sol.order_id
                    LEFT JOIN product_product prd
                        ON sol.product_id = prd.id
                    LEFT JOIN product_template pt
                        ON pt.id = prd.product_tmpl_id
                    LEFT JOIN res_partner prt
                        ON so.partner_id = prt.id
                    LEFT JOIN res_partner cust
                        ON so.customer_id = cust.id
                    LEFT JOIN res_partner_data rpd
                        ON rpd.partner_id = prt.id
                    WHERE so.date_order = %s
                    AND so.state NOT IN ('draft','cancel')
                    ORDER BY agent;
                    '''
            params = (str(orderDate),)
            orders = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return orders

    def format_xlsx(self, OrdersToday):
        '''
        Turn the received dictionary into an xlsx file to be sent out
        '''
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Order Number', 'Order Date',
                   'Forecasted Delivery Date',
                   'Item Number', 'Item Name',
                   'Qty Ordered', 'Agent', 'Customer',
                   'Route ID', 'Sale Value']
        active_sheet.append(headers)
        for order in OrdersToday:
            delivery = ["'" + str(item)
                        if isinstance(item, datetime) == True
                        else item
                        for item in order]
            active_sheet.append(order)
        result_file.save('todaysOrders.xlsx')


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
