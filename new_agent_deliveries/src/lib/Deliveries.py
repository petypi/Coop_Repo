"""
Main library for processing the deliveries intended for new agents
"""
import psycopg2
from datetime import datetime, timedelta
import pytz
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


class Deliveries(object):
    """
    Fetch newest product changes from the database and mail them to concerned
    party
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
            server.sendmail(EMAIL_SETTINGS['username'], toaddr,
                            message.as_string())
            server.close()
        except smtplib.SMTPException:
            raise

    def get_new_agent_deliveries(self):
        """
        Get the newest agents
        """
        agents = None
        tmz_today = datetime.now(pytz.timezone('Africa/Nairobi')).date()
        tomorrow = tmz_today + timedelta(days=1)
        try:
            sql = '''
            select so.date_order, so.date_delivery, rp.name, rp.phone, rp2.name, so.name, 
            sol.name, sol.product_uom_qty, sol.price_unit, sol.product_uom_qty * sol.price_unit Total, 
            del.name  from sale_order so left JOIN sale_order_line sol on sol.order_id=so.id 
            left join res_partner rp on rp.id = so.partner_id left join res_users ru on so.user_id = ru.id 
            left join res_partner rp2 on rp.sale_associate_id = rp2.id 
            left join res_partner_data rpd on rp.id = rpd.partner_id
            left join delivery_route del on rpd.route_id = del.id 
            where so.partner_id in (select so.partner_id from sale_order so left join res_partner rp 
            on rp.id=so.partner_id group by so.partner_id, rp.name having count(so.partner_id) <= 3)
                and so.date_delivery = %s;
        '''
            params = (str(tomorrow), )
            agents = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return agents

    def format_xlsx(self, deliveries):
        """
        Turn the received dictionary into an xlsx file to be sent out
        """
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Order Date', 'Forecasted Delivery Date', 'Agent Name', 'Agent Phone Number',
                   'Salesperson', 'Sale Order No', 'Product Name', 'Quantity', 'Unit Price', 'Total', 'Route Name']
        active_sheet.append(headers)
        for delivery in deliveries:
            delivery = ["'" + str(item)
                        if isinstance(item, datetime)
                        else item
                        for item in delivery]
            active_sheet.append(delivery)
        result_file.save('new_deliveries.xlsx')


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
