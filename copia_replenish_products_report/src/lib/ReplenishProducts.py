'''
Main library for processing the orders for the today, for the workload in the warehouse
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

CONFIG_READER = ConfigParser.ConfigParser()
CONFIG_READER.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
EMAIL_SETTINGS = dict(CONFIG_READER.items('email'))
APP_SETTINGS = dict(CONFIG_READER.items('data'))
DB_SETTINGS = dict(CONFIG_READER.items('database'))


class ReplenishProducts(object):
    '''
    Purchase Orders that are to be delivered today.
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

    def get_products(self):
        '''
        Get the newest agents
        '''
        products = None

        try:
            sql = '''with 
                        m_sale as (select  a.product_id, sum(a.quantity) as qty 
                            from account_invoice_line a
                            join account_invoice i on a.invoice_id=i.id
                            where i.state not in ('cancel') and i.number like ('SAJ%')
                            and date(i.date_invoice) >= now ()::date - interval '30 days'
                            and to_char(date(i.date_invoice), 'DAY') != 'SUNDAY' 
                            group by a.product_id
                        ),
                        q_in as (select m.product_id,m.location_dest_id, sum(m.product_qty/u.factor) as qty
                            from stock_move m
                            join product_uom u on u.id = m.product_uom
                            where m.location_id not in (12,61)
                            and m.location_dest_id in (12,61)
                            and m.state in ('done') 
                            group by m.product_id,m.location_dest_id
                        ),
                        q_out as (select m.product_id, sum(m.product_qty/u.factor) as qty
                            from stock_move m
                            join product_uom u on u.id = m.product_uom
                            where m.location_id in(12,61)
                            and m.location_dest_id not in (12,61)
                            and m.state in ('done') 
                            group by m.product_id
                        ),
                        pending as (
                        select
                            p.id as product_id, sum(l.product_uom_qty) as qty
                        from sale_order o
                        left join sale_order_line l on o.id = l.order_id
                        join product_product p on p.id = l.product_id
                        join product_template t on t.id = p.product_tmpl_id
                        where o.state = 'progress' and o.date_delivery > now ()
                        and date_order < now () 
                        group by p.id
                    )

                    select
                        ptl.name ProductName,
                        ptl.default_code Code,
                        pc.name Category,
                        psc.name SubCategory,
                        round(coalesce(i.qty, 0) - coalesce(o.qty, 0), 1) as qoh,
                        --aqo.qty_outbound Outbound,
                        --aqo.qoh + aqo.qty_inbound - aqo.qty_outbound AvailableQty,    
                        swo.product_min_qty Min,
                        swo.product_max_qty Max,
                        ms.qty/26 AvrgMonthlySales,
                        max(po.date_order) OrderDate,
                        round(cast(float8 (round(coalesce(i.qty, 0) - coalesce(o.qty, 0), 1)) as numeric)/(ms.qty/26),2) ReplenishCover,
                        case  
                            when (round(coalesce(i.qty, 0) - coalesce(o.qty, 0), 1)) >= swo.product_max_qty then 'Over Max'
                            when (round(coalesce(i.qty, 0) - coalesce(o.qty, 0), 1)) < swo.product_max_qty and
                                 (round(coalesce(i.qty, 0) - coalesce(o.qty, 0), 1)) >= swo.product_min_qty then 'Within Min/Max'
                            when (round(coalesce(i.qty, 0) - coalesce(o.qty, 0), 1)) < swo.product_min_qty then 'Under Min'
                        end as Comment
                    from
                        product_product pd
                        left join product_template ptl on ptl.id = pd.product_tmpl_id 
                        left join stock_warehouse_orderpoint swo on pd.id = swo.product_id
                        left join product_template t on pd.product_tmpl_id = t.id 
                        left join product_category psc on t.categ_id = psc.id 
                        left join product_category pc on psc.parent_id = pc.id
                        left join purchase_order_line pol on pd.id = pol.product_id
                        left join purchase_order po on pol.order_id = po.id
                        left join m_sale ms on pd.id = ms.product_id
                        left join q_in i on pd.id = i.product_id
                        left join q_out o on pd.id = o.product_id
                        left join pending oo on pd.id = oo.product_id
                    where 
                        swo.active = True and 
                        ptl.can_stock = True
                    group by ptl.name,
                             ptl.default_code,
                             pc.name,
                             psc.name,
                             i.qty,
                             o.qty,
                             swo.product_min_qty,
                             swo.product_max_qty,
                             ms.qty;
                '''
            products = self.db_management.retrieve_all_data_params(sql)
        except psycopg2.Error:
            raise
        return products

    def format_xlsx(self, products):
        '''
        Turn the received dictionary into an xlsx file to be sent out
        '''
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Product Name', 'Product Code', 'Category', 'SubCategory',
                   'QoH', 'Min', 'Max', 
                   'Avg Monthly Sales', 'Last Ordered', 'Replenish Cover', 'Comment']
        active_sheet.append(headers)
        for product in products:
            delivery = ["'" + str(item)
                        if isinstance(item, datetime) == True
                        else item
                        for item in product]
            active_sheet.append(product)
        result_file.save('ReplenishProductsScheduleReport.xlsx')


class DatabaseManagement(object):
    '''
    The basic class for DB management methods
    '''
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def retrieve_all_data_params(self, sql):
        '''
        Retrieve all data associated with the params from the database
        '''
        results = None
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
        except psycopg2.Error:
            raise
        return results

    def execute_query(self, sql):
        '''
        Run a query that does not require a return
        '''
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            self.db_connection.commit()
        except psycopg2.DatabaseError:
            raise
