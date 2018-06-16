"""
Main library for processing the Copia Institutions Data
"""
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


class Routing(object):
    """
    Fetch current balances from the database and mail them to concerned party
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

    def get_deliveries(self, delivery_date):
        """
        Get expected deliveries for the day after current date
        """
        deliveries = None
        try:
            sql = '''select so.date_delivery, so.partner_id, rp.name,
                   pl.name, sol.product_uom_qty, pp.default_code, sol.name, pt.weight, 
                   rpc.name, rp.phone, rpd.latitude,rpd.longitude, so.name, dr.name,
                   pc.name as product_category from sale_order so 
                   left join sale_order_line sol ON sol.order_id = so.id 
                   left join res_partner rp on rp.id = so.partner_id 
                   left join res_partner_data rpd on rp.id = rpd.partner_id
                   left join res_partner_location pl on pl.id = rpd.location_id 
                   left join res_partner_res_partner_category_rel rprpcr ON
                   rprpcr.partner_id = rp.id left join res_partner_category
                   rpc ON rpc.id = rprpcr.category_id 
                   left join product_product pp on pp.id = sol.product_id 
                   left join product_template pt on pp.product_tmpl_id = pt.id 
                   left join product_category pc on pc.id = pt.categ_id
                   left join delivery_route dr on rpd.route_id = dr.id 
                   where so.date_delivery = %s and so.state in ('done', 'sale') 
                   and rpc.name= 'Institution'
                '''
            inst_delivery_date = datetime.strftime(datetime.strptime(delivery_date, '%Y-%m-%d') +
                                                   timedelta(days=1), '%Y-%m-%d')

            if datetime.weekday(datetime.now()) == 4:
                inst_delivery_date = datetime.strftime(datetime.strptime(delivery_date, '%Y-%m-%d') +
                                                       timedelta(days=2), '%Y-%m-%d')
                params = (str(inst_delivery_date), )
                institution_deliveries = self.db_management.retrieve_all_data_params(sql, params)
                sql = '''select so.date_delivery, so.partner_id, rp.name, pl.name, 
                        sol.product_uom_qty, pp.default_code, sol.name, pt.weight, 
                        rpc.name, rp.phone, rpd.latitude,rpd.longitude, so.name, dr.name, 
                        pc.name as product_category from sale_order so 
                        left join sale_order_line sol ON sol.order_id = so.id 
                        left join res_partner rp on rp.id = so.partner_id 
                        left join res_partner_data rpd on rp.id = rpd.partner_id 
                        left join res_partner_location pl on pl.id = rpd.location_id 
                        left join res_partner_res_partner_category_rel rprpcr ON 
                            rprpcr.partner_id = rp.id 
                        left join res_partner_category rpc ON rpc.id = rprpcr.category_id 
                        left join product_product pp on pp.id = sol.product_id 
                        left join product_template pt on pp.product_tmpl_id = pt.id 
                        left join product_category pc on pc.id = pt.categ_id 
                        left join delivery_route dr on rpd.route_id = dr.id 
                        where so.date_delivery = %s and so.state in ('done', 'sale') 
                         and rpc.name in ('Vendor','TBCs') 
                     '''
                params = (str(delivery_date),)
                vendor_deliveries = self.db_management.retrieve_all_data_params(sql, params)
                deliveries = institution_deliveries + vendor_deliveries

        except psycopg2.Error:
            raise
        return deliveries

    def format_xlsx(self, deliveries):
        """
        Turn the received dictionary into an xlsx file to be sent out
        """
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Forecasted Delivery', 'Agent ID', 'Agent Name', 'Location', 'Quantity', 'Item Default Code',
                   'Product', 'Product Weight', 'Agent Category', 'Vendor Phone Number', 'Agent Latitude',
                   'Agent Longitude', 'Order Reference', 'Product Category', 'Agent Route']
        active_sheet.append(headers)
        if deliveries:
            for delivery in deliveries:
                active_sheet.append(delivery)
        result_file.save('routing_report.xlsx')


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
