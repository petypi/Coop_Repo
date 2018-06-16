"""
Main library for processing the orders for the today, for the workload in the warehouse
"""
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

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))


class ConfirmedOrders(object):
    """
    Purchase Orders that are to be delivered today.
    """
    def __init__(self, db_connection):
        self.db_management = DatabaseManagement(db_connection)

    def email_results(self, filename):
        """
        Send the files to the team involved
        """
        try:
            message = MIMEMultipart('alternative')
            message['From'] = "Copia Report Automation"
            toaddr = ast.literal_eval(EMAIL_SETTINGS['recipients'])
            message['Subject'] = EMAIL_SETTINGS['subject']
            message.attach(MIMEText("Dear Team,\nAttached is today's Purchase Orders."))
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

    def get_orders(self):
        """
        Get the newest agents
        """
        orders = None
        orderDate = datetime.today().date()
        try:
            sql = '''select wh.name Warehouse, prt.name Supplier, po.name
                     OrderNumber, po.date_order OrderDate,
                     po.date_planned DeliveryDate, prd.default_code
                     ItemNumber, pt.name ItemName, pol.product_qty QtyOrdered
                     from purchase_order po 
                     left join purchase_order_line pol on po.id = pol.order_id 
                     left join product_product prd on pol.product_id = prd.id 
                     left join product_template pt on prd.product_tmpl_id = pt.id
                     left join res_partner prt on po.partner_id = prt.id 
                     left join stock_picking_type sp on po.picking_type_id = sp.id
                     left join stock_warehouse wh on sp.warehouse_id = wh.id
                     where po.date_order > %s and po.state not in ('draft','cancel');
                     '''
            params = (str(orderDate), )
            orders = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return orders

    def format_xlsx(self, OrdersToday):
        """
        Turn the received dictionary into an xlsx file to be sent out
        """
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Warehouse', 'Supplier',
                   'Order Number','Order Date', 'Delivery Date', 'Item Number',
                   'Item Name','Quantity Ordered']
        active_sheet.append(headers)
        for order in OrdersToday:
            delivery = ["'" + str(item)
                        if isinstance(item, datetime) == True
                        else item
                        for item in order]
            active_sheet.append(order)
        result_file.save('todaysOrders.xlsx')


class DatabaseManagement(object):
    """
    The basic class for DB management methods
    """
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def retrieve_all_data_params(self, sql, params):
        """
        Retrieve all data associated with the params from the database
        """
        results = None
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql, params)
            results = cursor.fetchall()
        except psycopg2.Error:
            raise
        return results

    def execute_query(self, sql, params):
        """
        Run a query that does not require a return
        """
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql, params)
            self.db_connection.commit()
        except psycopg2.DatabaseError:
            raise
