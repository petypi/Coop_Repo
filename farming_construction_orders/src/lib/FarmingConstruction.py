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


class FarmingConstruction(object):
    '''
    Farming and Construction Orders
    '''
    def __init__(self, db_connection):
        self.db_management = DatabaseManagement(db_connection)

    def email_results(self, filename):
        '''
        Send the files to the teams
        '''
        try:
            message = MIMEMultipart('alternative')
            message['From'] = "Copia Report Automation"
            toaddr = ast.literal_eval(EMAIL_SETTINGS['recepients'])
            message['Subject'] = EMAIL_SETTINGS['subject']
            message.attach(MIMEText("Dear Team,\nKindly find attached today's Farming and Construction Orders."))
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
	if date.weekday(datetime.today())==0: # if today monday, we send orders with order date Sarturday
        	orderDate = datetime.today().date() - timedelta(days=2)
	else:
	        orderDate = datetime.today().date() - timedelta(days=1)
        try:
            sql = '''select
        so.date_order OrderDate,
        so.date_delivery DeliveryDate,
        agnt.name Agent,
        agnt.phone Phone,
        cust.name Customer,
        so.name OrderNumber,
        prd.default_code ProductCode,
        t.name ItemName,
        sol.product_uom_qty QtyOrdered,
        sol.price_unit UnitPrice,
        (sol.price_unit * sol.product_uom_qty) SaleValue,
        rpd.route_id Route,
        rt.name RouteName
        from
        sale_order_line sol
        left join sale_order so on so.id = sol.order_id
        left join product_product prd on sol.product_id = prd.id
        left join res_partner agnt on agnt.id = so.partner_id
        left join res_partner_data rpd on rpd.partner_id = agnt.id
        left join product_template t on t.id = prd.product_tmpl_id
        left join product_category c1 on c1.id = t.categ_id
        left join product_category c2 on c2.id = c1.parent_id
        left join res_partner cust on so.customer_id = cust.id
        left join delivery_route rt on rpd.route_id = rt.id
        where
        so.date_order::date = %s
        and so.state not in ('draft','cancel')
        and c2.name in ('Farm Inputs & Equipment', 'Construction Materials and Equipment')
        --and
        --prd.active
        order by agent;
                '''
            params = (str(orderDate), )
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
        headers = [
                'Order Date',
                'Forcast Delivery Date',
                'Agent Name',
                'Agent Phone Number',
                'Salesperson',
                'Sale Order No',
                'Product Code',
                'Product Name',
                'Quantity',
                'Unit Price',
                'Total',
                'Route ID',
                'Route Name',
                ]
        active_sheet.append(headers)
        for order in OrdersToday:
            delivery = ["'" + str(item)
                        if isinstance(item, datetime) == True
                        else item
                        for item in order]
            active_sheet.append(order)
        result_file.save('FarmingConstructionOrders.xlsx')


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
