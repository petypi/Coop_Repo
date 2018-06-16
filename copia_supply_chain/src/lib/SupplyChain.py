import psycopg2
from datetime import datetime, date, timedelta
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


class SupplyChain(object):
    def __init__(self, db_connection):
        self.db_management = DatabaseManagement(db_connection)

    def email_results(self, filename):
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

    def requisitions_vs_purchases(self):
        rvp = None
        try:
            sql = '''with requisitioned as (
                        select
                            l.product_id,
                            sum(l.product_qty) as qty,
                            u.name as uom_name,
                            sum(l.qty_to_purchase) as qty_2p,
                            sum(l.pending_in) as qty_pend_in,
                            sum(l.pending_out) as qty_pend_out,
                            avg(qty_available) as qoh
                        from purchase_requisition_line l
                        join purchase_requisition o on o.id = l.requisition_id
                        join product_uom u on u.id = l.product_uom_id
                        where date(o.create_date) = %s
                        group by l.product_id, u.id
                    ),
                    purchased as (
                        select
                            l.product_id,
                            sum(l.product_qty/u.factor) as qty
                        from purchase_order_line l
                        join purchase_order o on o.id = l.order_id
                        join product_uom u on u.id = l.product_uom
                        where date(o.create_date) = %s and o.state = 'approved'
                        group by l.product_id
                    )

                    select
                        p.default_code as "Code",
                        t.name as "Product",
                        c.name as "Category",
                        t.can_stock as "Stockable",
                        coalesce(round(r.qoh, 2), 0) as "QoH",
                        r.uom_name as "UoM",
                        coalesce(round(r.qty, 2), 0) as "Qty Ordered",
                        coalesce(r.qty_pend_out, 0) as "Pending Outgoing Qty",
                        coalesce(r.qty_pend_in, 0) as "Pending Incoming Shipments",
                        coalesce(r.qty_2p, 0) as "Qty to Purchase",
                        coalesce(round(b.qty, 2), 0) as "Qty Purchased",
                        date(now()) as "Date",
                        case
                            when round((b.qty - r.qty_2p)::numeric, 2) = 0 and b.qty > 0 then 'purchased'
                            when round((b.qty - r.qty_2p)::numeric, 2) < 0 and b.qty > 0 then 'less purchased'
                            when round((b.qty - r.qty_2p)::numeric, 2) > 0 and b.qty > 0 then 'more purchased'
                            when round((coalesce(b.qty,0) - coalesce(r.qty_2p,0))::numeric, 2) <= 0
                                and round((coalesce(r.qoh, 0))::numeric, 2) >= round(coalesce(r.qty, 0), 2) then 
                                'purchase not required'
                            else 'not purchased'
                        end as "State"

                    from requisitioned r
                    join product_product p on p.id = r.product_id
                    join product_template t on t.id = p.product_tmpl_id
                    join product_category c on c.id = t.categ_id
                    left join purchased b on b.product_id = r.product_id
                    order by p.default_code asc;'''
            today = date.today().__str__()
            params = (today, today,)
            rvp = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return rvp

    def purchases_vs_planned_inbounds(self):
        """
        Get what was purchased vs what was received after order confirmation
        """
        rvp = None
        try:
            sql = '''with purchased as (
                        select
                            r.name as partner,
                            o.name as po_num,
                            l.product_id, 
                            sum(l.product_qty/u.factor) as qty
                        from purchase_order_line l
                        join product_uom u on u.id = l.product_uom
                        join purchase_order o on o.id = l.order_id
                        join res_partner r on r.id = o.partner_id
                        where date(o.create_date) = %s and o.state = 'approved'
                        group by r.id, o.id, l.product_id
                    ),
                    pln_q_in as (
                        select 
                            m.product_id, sum(m.product_qty/u.factor) as qty
                        from stock_move m
                        join product_uom u on u.id = m.product_uom
                        join stock_picking k on k.id = m.picking_id
                        join purchase_order_stock_picking_rel rel on rel.stock_picking_id = k.id
                        join purchase_order o on o.id = rel.purchase_order_id
                        where m.location_id != 12
                        and m.location_dest_id = 12
                        and m.state in ('done')
                        and date(o.date_planned) = %s
                        group by m.product_id
                    )
                    
                    select
                        b.partner as "Supplier",
                        b.po_num as "Purchase Order No.",
                        p.default_code as "Code",
                        t.name as "Product",
                        coalesce(round(b.qty, 2), 0) as "Purchased Qty",
                        coalesce(round(pln_q_in.qty, 2), 0) as "Qty Received",
                        date(now()) as "Date",
                        case
                            when round((pln_q_in.qty - b.qty)::numeric, 2) = 0 then 'received'
                            when round((pln_q_in.qty - b.qty)::numeric, 2) < 0 then 'less received'
                            when round((pln_q_in.qty - b.qty)::numeric, 2) > 0 then 'more received'
                            else 'not received'
                        end as state
                    from purchased b
                    left join product_product p on p.id = b.product_id
                    left join product_template t on t.id = p.product_tmpl_id
                    left join pln_q_in on pln_q_in.product_id = b.product_id'''
            today = date.today().__str__()
            params = (today, today,)
            # today_oc = datetime.strptime('%s %s' % (today, '09:30:00'), '%Y-%m-%d %H:%M:%S').__str__()
            # today_end = datetime.strptime('%s %s' % (today, '11:59:59'), '%Y-%m-%d %H:%M:%S').__str__()
            # params = (today, today_oc, today_end, today,)
            rvp = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return rvp

    def available_qty_vs_outbound(self):
        avo = None
        try:
            sql = '''
            with requisitioned as (
                    select
                        l.product_id, sum(l.product_qty/u.factor) as qty, avg(qty_available) as qoh
                    from purchase_requisition_line l
                    join purchase_requisition o on o.id = l.requisition_id
                    join product_uom u on u.id = l.product_uom_id
                    where date(o.create_date) = %s
                    group by l.product_id
                ),
                -- Inbounds for the day
                inbound as (
                    SELECT
                        m.product_id, SUM(m.product_qty/u.factor) as qty
                    FROM stock_move m
                    JOIN product_uom u ON u.id = m.product_uom
                    JOIN stock_picking k ON k.id = m.picking_id
                    WHERE m.location_id != 13
                    AND m.location_dest_id = 13
                    AND m.state in ('done')
                    AND DATE(k.scheduled_date) = %s
                    GROUP BY m.product_id
                ),
                -- Outbound for the day
                outbound as (
                    select
                        l.product_id, sum(l.product_uom_qty) as qty
                    from sale_order_line l
                    join sale_order o on o.id = l.order_id
                    and o.state in ('done', 'progress')
                    and date(o.date_delivery) = %s
                    group by l.product_id
                )

                select
                    p.default_code as "Code",
                    pt.name as "Product Name",
                    coalesce(round(r.qoh, 2), 0) as "QoH at Confirmation",
                    coalesce(round(i.qty, 2), 0) as "Day's Inbounds",
                    round(coalesce(r.qoh, 0) + coalesce(i.qty, 0), 2) as "Available Stock",
                    coalesce(round(o.qty, 2), 0) as "Outbound",
                    %s as "Outbound Delivery Date",
                    %s as "Report Date"
                from requisitioned r
                join product_product p on p.id = r.product_id
                left join product_template pt on pt.id = p.product_tmpl_id
                left join inbound i on i.product_id = r.product_id
                left join outbound o on o.product_id = r.product_id
                order by p.default_code asc;'''

            today =  date.today()
            delivery_date = (today + timedelta(days=1))

            if delivery_date.weekday() == 6:
                delivery_date = delivery_date + timedelta(days=1)

            params = (
                today.__str__(), today.__str__(), delivery_date.__str__(),
                delivery_date.__str__(), today.__str__(),
            )
            avo = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return avo

    def delivery_manifest(self):
        """
        Get what was dispatched for the day
        """
        rvp = None
        try:
            sql = '''
                select
                    o.name, 
                    o.state,
                    o.date_order, 
                    o.confirmation_date,
                    o.date_delivery, 
                    r.name, 
                    t.name,
                    i.number,
                    i.receipt_ref,
                    i.state,
                    p.default_code, 
                    pt.name,
                    il.quantity
                from sale_order o
                join sale_order_line ol on o.id = ol.order_id
                join res_partner r on r.id = o.partner_id
                join res_partner_data rd on rd.partner_id = r.id
                join delivery_route t on t.id = rd.route_id
                join sale_order_line_invoice_rel rel on rel.order_line_id = ol.id
                join account_invoice_line il on il.id = rel.invoice_line_id
                join account_invoice i on i.id = il.invoice_id
                join product_product p on p.id = il.product_id
                join product_template pt on pt.id = p.product_tmpl_id
                where o.state in ('done', 'progress') 
                and date(o.confirmation_date) = %s
                and date(o.date_delivery) = %s
                order by o.state asc, t.name asc, r.name asc, o.name asc, p.default_code asc;
            '''
            today = date.today()
            # today = datetime.strptime('2017-07-29', '%Y-%m-%d').date()
            delivery_date = (today + timedelta(days=1))

            if delivery_date.weekday() == 6:
                delivery_date = delivery_date + timedelta(days=1)

            params = (today.__str__(), delivery_date.__str__(),)
            dm = self.db_management.retrieve_all_data_params(sql, params)
        except psycopg2.Error:
            raise
        return dm

    def format_csv(self, deliveries, filename, headers):
        """
        Turn the received dictionary into a CSV file to be sent out
        """
        result_file = csv.writer(open(filename, 'wb'))
        deliveries.insert(0, headers)
        for delivery in deliveries:
            result_file.writerow(delivery)

    def format_xlsx(self, deliveries):
        """
        Turn the received dictionary into an xlsx file to be sent out
        """
        result_file = Workbook()
        active_sheet = result_file.active
        headers = ['Code', 'Product Name' ,'Qty on Hand',
                   'Qty Requisitioned', 'Qty Purchased', 'Status']
        active_sheet.append(headers)
        for delivery in deliveries:
            active_sheet.append(delivery)
        result_file.save('requisitions_vs_purchases.xlsx')


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
