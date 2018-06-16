"""
Main library for processing the Copia price changes
"""
import psycopg2
from datetime import datetime
import csv
from Postgres import Postgres
from email.mime.multipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email import Encoders
import ast
import smtplib
import ConfigParser
import os
from datetime import datetime
from openpyxl import Workbook

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))


class Changes(object):
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

    def get_price_changes(self):
        """
        Get Products whose prices have been changed
        """
        agents = None
        try:
            sql = '''select alog.write_date, alog.name, alogl.old_value, 
                    alogl.new_value, rp.name from auditlog_log_line alogl 
                    left join auditlog_log alog on alog.id = alogl.log_id 
                    left join res_users ru on alog.user_id = ru.id 
                    left join res_partner rp on ru.partner_id = rp.id 
                    left join ir_model on ir_model.id = alog.model_id
                    left join ir_model_fields fld on fld.id = alogl.field_id
                    where ir_model.name = %s 
                    and fld.name = %s and 
                    alog.write_date >= (current_date - integer %s) 
                    order by alog.write_date desc;''' 
            params = ('Product Template', 'list_price', str(APP_SETTINGS['period']), )
            agents = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return agents

    def format_csv(self, deliveries):
        """
        Turn the received  dictionary into a CSV file to be sent out
        """
        result_file = csv.writer(open('/tmp/price_changes.csv', 'wb'))
        headers = ['Date Changed' ,'Product Name', 'Old Price', 
                   'New Price', 'Changed By']
        deliveries.insert(0, headers)
        for delivery in deliveries:
            delivery = ["'" + str(item) if type(item) == datetime else item for item in delivery]
            result_file.writerow(delivery)

    def format_xlsx(self, deliveries):
        """
        Turn the received dictionary into an xlsx file to be sent out
        """
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Date Changed' ,'Product Name', 'Old Price',
                   'New Price', 'Changed By']
        active_sheet.append(headers)
        for delivery in deliveries:
            delivery = ["'" + str(item) if type(item) == datetime else item for item in delivery]
            active_sheet.append(delivery)
        result_file.save('price_changes.xlsx')


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
