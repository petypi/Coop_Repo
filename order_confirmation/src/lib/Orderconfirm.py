"""
Main library for processing the deliveries intended for new agents
"""
import psycopg2
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import ast
import smtplib
import ConfigParser
import os

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))


class Orderconfirm(object):
    """
    Fetch newest product changes from the database and mail them to concerned party
    """
    def __init__(self, db_connection):
        self.db_management = DatabaseManagement(db_connection)

    def email_results(self):
        """
        Send the files to the team involved
        """
        try:
            message = MIMEMultipart('alternative')
            message['From'] = "Copia Report Automation"
            toaddr = ast.literal_eval(EMAIL_SETTINGS['recepients'])
            message['Subject'] = EMAIL_SETTINGS['subject']
            message.attach(MIMEText("Dear Team,\nOrder Confirmation Is Done"))
            attachment = MIMEBase('application', 'octet-stream')
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

    def confirm_orders(self):
        """
        Get the orders with total price > 10000
        """
        resp = None
        try:
            stored_procedure_name = 'proc_confirmorders'
            dayofweek = datetime.today().weekday()
            jana = datetime.today().date()
            if dayofweek != 5:
                if dayofweek == 6:
                    jana = datetime.today().date() - timedelta(days=1)
                print jana
                params = (str(jana), 'true',)
                resp = self.db_management.call_func(stored_procedure_name, params)
        except psycopg2.Error:
            raise
        return resp


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

    def call_func(self, stored_procedure_name, params):
        """
        call a stored function
        """
        try:
            cursor = self.db_connection.cursor()
            resp = cursor.callproc(stored_procedure_name, params)
            self.db_connection.commit()
        except psycopg2.DatabaseError:
            raise
        return resp
