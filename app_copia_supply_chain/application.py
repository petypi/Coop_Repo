#! /usr/bin/env python2
# Application

import os
import sys
from datetime import datetime, date, timedelta
import time
import pytz
from sqlalchemy import text, exists, and_
from lib.utility import Utility
from lib.db import DB
from lib.models import ConfirmedSaleOrder, RequisitionVsPurchase, PurchaseVsPlannedInbound, \
AvailableQtyVsOutbound, DeliveryManifest, DeliveryReturns, HistoricalQoH
from lib.database import Database

utility = Utility()
database = DB()
wrt_session = database.push_session()


def today_confirmed_order_lines(qry_date=False):
    vals = []
    d, n = 0, 0
    copia_date_order = qry_date or date.today().__str__()
    params = (copia_date_order, copia_date_order,)

    sql = text("""
        select
            so.id as so_id,
            so.name as so_name,
            so.date_order as date_order,
            so.date_delivery as date_delivery,
            prd.id as product_id,
            prd.default_code as product_code,
            pt.name as product_name,
            sol.product_uom_qty as qty_ordered,
            prt.id as agent_id,
            prt.name as agent_name,
            cust.id as customer_id,
            cust.name as customer,
            rt.id as route_id,
            rt.name,
            sol. id as sol_id,
            (sol.price_unit * sol.product_uom_qty) as sol_value,
            so.state as so_state,
            '%s' as date_report
        from sale_order so
        join sale_order_line sol on so.id = sol.order_id
        join product_product prd on sol.product_id = prd.id
        join product_template pt on prd.product_tmpl_id =pt.id
        join res_users ru on so.user_id = ru.id
        join res_partner prt on ru.partner_id = prt.id
        join res_partner cust on so.customer_id = cust.id
        join res_partner rp on so.partner_id=rp.id
        join res_partner_data rpd ON rpd.partner_id = rp.id
        join delivery_route rt on rt.id = rpd.route_id
        where so.date_order = '%s'
        and so.state in ('done','sale') order by prt.id;
    """ % params)

    result = database.pull_conn.execute(sql)
    for row in result:
        if wrt_session.query(ConfirmedSaleOrder.so_id).filter_by(sol_id=row[14]).scalar() is not None:
            d += 1
        else:
            val = ConfirmedSaleOrder(
                so_id=row[0], so_name=row[1], date_order=row[2], date_delivery=row[3], product_id=row[4],
                product_code=row[5], product_name=row[6], qty_ordered=row[7], agent_id=row[8], agent_name=row[9],
                customer_id=row[10], customer_name=row[11], route_id=row[12], route_name=row[13], sol_id=row[14],
                sol_value=row[15], so_state=row[16], date_report=row[17]
            )
            wrt_session.add(val)
            n += 1

    wrt_session.commit()
    utility.logger.info("Inserted Rows: %s" % n)
    utility.logger.info("Duplicate Rows: %s" % d)


def requisition_vs_purchase(qry_date=False):
    vals = []
    d, n = 0, 0
    copia_date_order = qry_date or date.today().__str__()

    sql = text("""
        with requisitioned as (
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
            where date(o.create_date) = '%s'
            group by l.product_id, u.id
        ),
        purchased as (
            select
                l.product_id,
                sum(l.product_qty/u.factor) as qty
            from purchase_order_line l
            join purchase_order o on o.id = l.order_id
            join product_uom u on u.id = l.product_uom
            where date(o.create_date) = '%s' and o.state = 'approved'
            group by l.product_id
        )

        select
            p.id as product_id,
            p.default_code as product_code,
            t.name as product_name,
            c.name as product_categ_name,
            t.can_stock,
            coalesce(round(r.qoh, 2), 0) as qoh,
            r.uom_name,
            coalesce(round(r.qty, 2), 0) as qty_ordered,
            coalesce(r.qty_pend_out, 0) as pending_out_qty,
            coalesce(r.qty_pend_in, 0) as pending_in_qty,
            coalesce(r.qty_2p, 0) as qty_2_purchase,
            coalesce(round(b.qty, 2), 0) as qty_purchased,
            case
                when round((b.qty - r.qty_2p)::numeric, 2) = 0 and b.qty > 0 then 'purchased'
                when round((b.qty - r.qty_2p)::numeric, 2) < 0 and b.qty > 0 then 'less purchased'
                when round((b.qty - r.qty_2p)::numeric, 2) > 0 and b.qty > 0 then 'more purchased'
                when round((coalesce(b.qty,0) - coalesce(r.qty_2p,0))::numeric, 2) <= 0
                    and round(coalesce(r.qoh, 0), 2) >= round(coalesce(r.qty, 0), 2) then 'purchase not required'
                else 'not purchased'
            end as state,
            date(now()) as date_report

        from requisitioned r
        join product_product p on p.id = r.product_id
        join product_template t on t.id = p.product_tmpl_id
        join product_category c on c.id = t.categ_id
        left join purchased b on b.product_id = r.product_id
        order by p.default_code asc;
    """ % (copia_date_order, copia_date_order))

    result = database.pull_conn.execute(sql)

    # TODO - Change the copia_date_order to row[13] on live
    for row in result:
        if wrt_session.query(RequisitionVsPurchase.product_id).filter(
                and_(RequisitionVsPurchase.product_id == row[0],
                     RequisitionVsPurchase.date_report == copia_date_order)).scalar() is not None:
            d += 1
        else:
            val = RequisitionVsPurchase(
                product_id=row[0], product_code=row[1], product_name=row[2], product_categ_name=row[3],
                can_stock=row[4],
                qoh=row[5], uom_name=row[6], qty_ordered=row[7], pending_out_qty=row[8], pending_in_qty=row[9],
                qty_2_purchase=row[10], qty_purchased=row[11], state=row[12], date_report=copia_date_order
            )
            wrt_session.add(val)
            n += 1

    wrt_session.commit()
    utility.logger.info("Inserted Rows: %s" % n)
    utility.logger.info("Duplicate Rows: %s" % d)


def purchase_planned_inbound(qry_date=False):
    vals = []
    d, n = 0, 0
    copia_date_order = qry_date or date.today().__str__()
    params = (copia_date_order, copia_date_order, copia_date_order,)

    sql = text("""
        with purchased as (
            select
                r.id as supplier_id,
                r.name as partner,
                o.id as po_id,
                o.name as po_num,
                l.product_id,
                sum(l.product_qty/u.factor) as qty
            from purchase_order_line l
            join product_uom u on u.id = l.product_uom
            join purchase_order o on o.id = l.order_id
            join res_partner r on r.id = o.partner_id
            where date(o.create_date) = '%s' and o.state = 'approved'
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
            and date(o.date_planned) = '%s'
            group by m.product_id
        )

        select
            b.supplier_id,
            b.partner as supplier_name,
            b.po_id,
            b.po_num as po_name,
            p.id as product_id,
            p.default_code as product_code,
            pt.name as product_name,
            coalesce(round(b.qty, 2), 0) as qty_purchased,
            coalesce(round(pln_q_in.qty, 2), 0) as qty_received,
            case
                when round(pln_q_in.qty - b.qty, 2) = 0 then 'received'
                when round(pln_q_in.qty - b.qty, 2) < 0 then 'less received'
                when round(pln_q_in.qty - b.qty, 2) > 0 then 'more received'
                else 'not received'
            end as state,
            '%s' as date_report
        from purchased b
        left join product_product p on p.id = b.product_id
        left join product_template pt on pt.id = p.product_tmpl_id
        left join pln_q_in on pln_q_in.product_id = b.product_id;
    """ % params)

    result = database.pull_conn.execute(sql)

    # TODO - Change the copia_date_order to row[13] on live
    for row in result:
        if wrt_session.query(
                PurchaseVsPlannedInbound.supplier_id).filter(and_(PurchaseVsPlannedInbound.supplier_id == row[0],
                                                                  PurchaseVsPlannedInbound.product_id == row[4],
                                                                  PurchaseVsPlannedInbound.date_report ==
                                                                  copia_date_order)).scalar() is not None:
            d += 1
        else:
            val = PurchaseVsPlannedInbound(
                supplier_id=row[0], supplier_name=row[1], po_id=row[2], po_name=row[3], product_id=row[4],
                product_code=row[5], product_name=row[6], qty_purchased=row[7], qty_received=row[8], state=row[9],
                date_report=copia_date_order
            )
            wrt_session.add(val)
            n += 1

    wrt_session.commit()
    utility.logger.info("Inserted Rows: %s" % n)
    utility.logger.info("Duplicate Rows: %s" % d)


def available_qty_outbound(qry_date=False):
    vals = []
    d, n = 0, 0
    copia_date_order = datetime.strptime(qry_date, "%Y-%m-%d").date() or date.today()

    delivery_date = copia_date_order + timedelta(days=1)

    if delivery_date.weekday() == 6:
        delivery_date = delivery_date + timedelta(days=1)

    params = (
        copia_date_order.__str__(), copia_date_order.__str__(), delivery_date.__str__(),
        delivery_date.__str__(), copia_date_order.__str__(),
    )

    sql = text("""
        with requisitioned as (
            select
                l.product_id, sum(l.product_qty/u.factor) as qty, avg(qty_available) as qoh
            from purchase_requisition_line l
            join purchase_requisition o on o.id = l.requisition_id
            join product_uom u on u.id = l.product_uom_id
            where date(o.create_date) = '%s'
            group by l.product_id
        ),
        -- Inbounds for the day
        inbound as (
            select
                m.product_id, sum(m.product_qty/u.factor) as qty
            from stock_move m
            join product_uom u on u.id = m.product_uom
            join stock_picking k on k.id = m.picking_id
            where m.location_id != 12
            and m.location_dest_id = 12
            and m.state in ('done')
            and date(k.scheduled_date) = '%s'
            group by m.product_id
        ),
        -- Outbound for the day
        outbound as (
            select
                l.product_id, sum(l.product_uom_qty) as qty
            from sale_order_line l
            join sale_order o on o.id = l.order_id
            and o.state in ('done', 'progress')
            and date(o.date_delivery) = '%s'
            group by l.product_id
        )

        select
            p.id as product_id,
            p.default_code as product_code,
            pt.name as product_name,
            coalesce(round(r.qoh, 2), 0) as qoh,
            coalesce(round(i.qty, 2), 0) as qty_inbound,
            round(coalesce(r.qoh, 0) + coalesce(i.qty, 0), 2) as qty_available,
            coalesce(round(o.qty, 2), 0) as qty_outbound,
            '%s' as date_delivery,
            '%s' as date_report
        from requisitioned r
        join product_product p on p.id = r.product_id
        join product_template pt on p.product_tmpl_id=pt.id
        left join inbound i on i.product_id = r.product_id
        left join outbound o on o.product_id = r.product_id
        order by p.default_code asc;
    """ % params)

    result = database.pull_conn.execute(sql)

    # TODO - Change the copia_date_order to row[13] on live
    for row in result:
        if wrt_session.query(AvailableQtyVsOutbound.product_id
                             ).filter(and_(AvailableQtyVsOutbound.product_id == row[0],
                                           AvailableQtyVsOutbound.date_report == copia_date_order)
                                      ).scalar() is not None:
            d += 1
        else:
            val = AvailableQtyVsOutbound(
                product_id=row[0], product_code=row[1], product_name=row[2], qoh=row[3], qty_inbound=row[4],
                qty_available=row[5], qty_outbound=row[6], date_delivery=row[7], date_report=row[8]
            )
            wrt_session.add(val)
            n += 1

    wrt_session.commit()
    utility.logger.info("Inserted Rows: %s" % n)
    utility.logger.info("Duplicate Rows: %s" % d)


def delivery_manifest(qry_date=False):
    vals = []
    d, n = 0, 0
    copia_date_order = datetime.strptime(qry_date, "%Y-%m-%d").date() or date.today()

    delivery_date = copia_date_order + timedelta(days=1)

    if delivery_date.weekday() == 6:
        delivery_date = delivery_date + timedelta(days=1)

    params = (copia_date_order.__str__(), copia_date_order.__str__(), delivery_date.__str__())

    sql = text("""
        select
            o.id as so_id,
            o.name as so_name,
            o.state as so_state,
            o.date_order,
            o.confirmation_date,
            o.date_delivery,
            r.id as agent_id,
            r.name as agent_name,
            t.id as route_id,
            t.name,
            i.id as invoice_id,
            i.number as invoice_number,
            i.receipt_ref,
            i.state as invoice_state,
            p.id as product_id,
            p.default_code as product_code,
            pt.name as product_name,
            l.quantity as qty_invoice,
            '%s' as date_report,
            i.date_invoice
        from sale_order o
        left join res_users ru on o.user_id = ru.id
        join res_partner r on ru.partner_id = r.id
        join res_partner_data rpd ON rpd.partner_id = r.id
        join delivery_route t on t.id = rpd.route_id
        join sale_order_line_invoice_rel rel on rel.order_line_id = o.id
        join account_invoice i on i.id = rel.invoice_line_id
        join account_invoice_line l on l.invoice_id = i.id
        join product_product p on p.id = l.product_id
        join product_template pt on p.product_tmpl_id =pt.id
        where o.state in ('done', 'progress')
        and date(o.confirmation_date) = '%s'
        and date(o.date_delivery) = '%s'
        order by o.state asc, t.name asc, r.name asc, o.name asc, p.default_code asc;
    """ % params)

    result = database.pull_conn.execute(sql)

    # TODO - Change the copia_date_order to row[13] on live
    for row in result:
        if wrt_session.query(DeliveryManifest.so_id).filter(and_(DeliveryManifest.so_id == row[0],
                                                                 DeliveryManifest.product_id == row[14],
                                                                 DeliveryManifest.date_report == copia_date_order)
                                                            ).scalar() is not None:
            d += 1
        else:
            val = DeliveryManifest(
                so_id=row[0], so_name=row[1], so_state=row[2], date_order=row[3], date_confirm=row[4],
                date_delivery=row[5], agent_id=row[6], agent_name=row[7], route_id=row[8], route_name=row[9],
                invoice_id=row[10], invoice_number=row[11], receipt_ref=row[12], invoice_state=row[13],
                product_id=row[14], product_code=row[15], product_name=row[16], qty_invoice=row[17],
                date_invoice=row[19], date_report=copia_date_order
            )
            wrt_session.add(val)
            n += 1

    wrt_session.commit()
    utility.logger.info("Inserted Rows: %s" % n)
    utility.logger.info("Duplicate Rows: %s" % d)


def delivery_returns(qry_date=False):
    vals = []
    d, n = 0, 0
    copia_date_order = datetime.strptime(qry_date, "%Y-%m-%d").date() or date.today()
    params = (copia_date_order.__str__(), copia_date_order.__str__(),)

    sql = text("""
            SELECT
                so.id AS so_id,
                so.name AS so_name,
                rp.id AS agent_id,
                rp.name AS agent_name,
                cc.code,
                pp.id AS product_id,
                pp.default_code AS product_code,
                pt.name AS product_name,
                cl.product_returned_quantity,
                (cl.unit_sale_price * cl.product_returned_quantity) AS return_value,
                rr.reason_code,
                cc.create_date AS date_create,
                '%s' AS date_report
            FROM product_return cc
            JOIN product_return_line cl 
                ON cc.id = cl.product_return_id
            JOIN return_reason rr  
                ON rr.id = cl.return_reason_id 
            JOIN res_partner rp  
                ON rp.id = cc.partner_id
            JOIN sale_order so  
                ON so.id = split_part(cc.ref, ',', 2)::INTEGER
            JOIN product_product pp  
                ON pp.id = cl.product_id
            JOIN product_template pt  
                ON pt.id = pp.product_tmpl_id
            WHERE DATE(cc.date) = '%s';
            """ % params)

    result = database.pull_conn.execute(sql)

    for row in result:
        if wrt_session.query(DeliveryReturns.so_id
                             ).filter(and_(DeliveryReturns.so_id == row[0],
                                           DeliveryReturns.product_id == row[5],
                                           DeliveryReturns.date_report == copia_date_order)).scalar() is not None:
            d += 1
        else:
            val = DeliveryReturns(
                so_id=row[0], so_name=row[1], agent_id=row[2], agent_name=row[3], return_no=row[4],
                product_id=row[5], product_code=row[6], product_name=row[7], product_returned_quantity=row[8],
                product_return_value=row[9], claim_origine=row[10], date_create=row[11], date_report=copia_date_order
            )
            wrt_session.add(val)
            n += 1

    wrt_session.commit()
    utility.logger.info("Inserted Rows: %s" % n)
    utility.logger.info("Duplicate Rows: %s" % d)


def historical_qoh(qry_date=False):
    vals = []
    d, n = 0, 0
    date_report = datetime.strptime(qry_date, "%Y-%m-%d").date() or date.today()
    params = (date_report.__str__(), date_report.__str__(), date_report.__str__())

    sql = text("""
        with sales as (
            select
                m.product_id, sum(m.product_qty/u.factor) as qty
            from stock_move m
            join product_uom u on u.id = m.product_uom
            where m.location_id = 12
            and m.location_dest_id <> 12
            and m.state in ('done')
            and date(m.write_date) >= date('%s') - interval '30 days'
            and to_char(date(m.write_date), 'DAY') != 'SUNDAY'
            group by m.product_id
        )

        select
            p.id as product_id,
            p.default_code as product_code,
            pt.name as product_name,
            rl.qty_available as qty_on_hand,
            round(rl.product_qty/u.factor, 1) as qty_ordered,
            coalesce(round(s.qty/26, 1), 0) as avg_4_wk_sales,
            '%s' as date_report
        from purchase_requisition_line rl
        join purchase_requisition r on r.id = rl.requisition_id
        join sales s on s.product_id = rl.product_id
        join product_product p on p.id = rl.product_id
        join product_template pt on p.product_tmpl_id =pt.id
        join product_uom u on u.id = rl.product_uom_id
        where date(r.create_date) = '%s'
        order by p.default_code asc;
    """ % params)

    _start = time.time()
    result = database.pull_conn.execute(sql)
    _first = time.time() - _start

    _start = time.time()
    for row in result:
        if wrt_session.query(HistoricalQoH.product_id
                             ).filter(and_(HistoricalQoH.product_id == row[0],
                                           HistoricalQoH.date_report == date_report)).scalar() is not None:
            d += 1
        else:
            val = HistoricalQoH(
                product_id=row[0], product_code=row[1], product_name=row[2], qty_on_hand=row[3],
                qty_ordered=row[4], avg_4_wk_sale=row[5], date_report=date_report
            )
            wrt_session.add(val)
            n += 1

    wrt_session.commit()
    _second = time.time() - _start

    utility.logger.info("Inserted Rows: %s" % n)
    utility.logger.info("Duplicate Rows: %s" % d)
    utility.logger.info("Fetch: %s Seconds" % _first)
    utility.logger.info("Insert: %s Seconds" % _second)


def daily_layaway_orders(qry_date=False):
    _date = qry_date and datetime.strptime(qry_date, "%Y-%m-%d").date() \
        or date.today().__str__()
    sql = text("""
        select
            o.id as "Order ID",
            o.name as "Reference",
            a.id as "Agent ID",
            a.name as "Agent",
            c.id as "Customer ID",
            c.name as "Customer",
            s.name as "Sales Person",
            o.date_order as "Ordered",
            o.confirmation_date::date as "Confirmed",
            o.date_delivery as "Delivery Date",
            o.amount_total as "Order Total",
            (
                coalesce(
                    (select sum(coalesce(amount, 0))
                    from account_payment where order_id = o.id
                    group by o.id), 0)
                    ) as "Prepayments"
        from sale_order o
        join sale_order_line l on l.order_id = o.id
        left join res_users ru on o.user_id = ru.id
        join res_partner a on ru.partner_id = a.id
        join res_partner c on c.id = o.customer_id
        left join res_users u on u.id = a.user_id
        left join res_partner s on s.id = o.sale_associate_id
        where o.confirmation_date::date = '%s' and islayaway is true
        and o.state = 'sale'
        group by a.id, o.id, c.id, s.id
        order by a.name asc, o.name asc
    """ % _date)

    utility.make_format_csv(
        [r for r in database.pull_conn.execute(sql)],
        "confirmed_daily_layaway_orders.csv",
        [
            "ID", "Reference", "Agent ID", "Agent", "Customer ID", "Customer", "Sales Person",
            "Ordered", "Confirmed", "Delivery Date", "Order Total", "Prepayment Total"
        ]
    )
    utility.send_email("confirmed_daily_layaway_orders.csv", "grp_1")
    os.remove("confirmed_daily_layaway_orders.csv")


def logistics_optiflow_report(qry_date=False):
    db = Database()
    if qry_date:
        _date = (datetime.strptime(qry_date, "%Y-%m-%d"))
        print _date
    else:
        #get the date today (tz aware)
        tmz_today = datetime.now(pytz.timezone('Africa/Nairobi')).date()
        _date = tmz_today + timedelta(days=1)
        print _date
    if _date.weekday() == 6:
        _date = _date + timedelta(days=1)
        print _date    
    sql = """
            SELECT
                o.name "Order Number",
                o.date_order::date "Order Date",
                o.confirmation_date::date "Confirm Date",
                o.date_delivery "Forecasted Delivery Date",
                pp.default_code "Item Number",
                pt.name "Item Name",
                SUM(COALESCE(ol.product_uom_qty, 0)) "Qty Ordered",
                p.name "Agent",
                c.name "Customer",
                r.id "Route ID",
                SUM(COALESCE(ol.product_uom_qty, 0) * COALESCE(ol.price_unit, 0)) "Sale Value",
                pc.name "Category",
                psc.name "Sub Category",
                SUM(COALESCE(pt.weight, 0)) "Weight",
                r.name "Route",
                rpd.latitude "Geo Latitude",
                rpd.longitude "Geo Longitude",
                rgn.name "Depot",
                p.id "Agent ID",
                l.id "Location ID",
                l.name "Location"
            FROM sale_order o
            LEFT JOIN res_partner p
                ON o.partner_id = p.id
            LEFT JOIN res_users ru
                ON o.user_id = ru.id
            LEFT JOIN res_partner p2
                ON ru.partner_id = p2.id
            LEFT JOIN sale_order_line ol
                ON ol.order_id = o.id
            LEFT JOIN product_product pp
                ON pp.id = ol.product_id
            LEFT JOIN product_template pt
                ON pt.id = pp.product_tmpl_id
            LEFT JOIN product_category pc
                ON pc.id = pt.categ_id
            LEFT JOIN product_category psc
                ON psc.id = pc.parent_id
            LEFT JOIN res_partner_data rpd
                ON rpd.partner_id = p.id
            LEFT JOIN res_partner_location l
                ON l.id = rpd.location_id
            LEFT JOIN stock_warehouse s
                ON s.id = o.warehouse_id
            LEFT JOIN res_partner c
                ON c.id = o.customer_id
            LEFT JOIN delivery_route r
                ON r.id = rpd.route_id
            LEFT JOIN delivery_region rgn
                ON rgn.id = r.region_id
            WHERE o.state = 'sale'
            AND o.date_delivery = '{}'
            GROUP BY o.id, pp.id, pc.id, psc.id, p.id, r.id, rgn.id,
                l.id, s.id, c.id, pt.name, p2.name, rpd.latitude, rpd.longitude
            ORDER BY p.name ASC;
            """.format(_date.strftime('%Y-%m-%d'))

    utility.make_format_csv(
        [r for r in db.fetchall(sql)],
        "optiflow_report.csv",
        [
            "Order Number", "Order Date", "Confirm Date", "Forecasted Delivery Date",
            "Item Number", "Item Name", "Qty Ordered", "Agent", "Customer", "Route ID",
            "Sale Value", "Category", "Sub Category", "Weight", "Route", "Geo Latitude",
            "Geo Longitude", "Depot", "Agent ID", "Location ID", "Location"
        ]
    )
    utility.send_email("optiflow_report.csv", "grp_2")
    os.remove("optiflow_report.csv")

def optiflow_redeliveries_report(qry_date=False):
    db = Database()
    if qry_date:
        _date = (datetime.strptime(qry_date, "%Y-%m-%d"))
        print _date
    else:
        # get the date today (tz aware)
        tmz_today = datetime.now(pytz.timezone('Africa/Nairobi')).date()
        _date = tmz_today
        # print _date
    if _date.weekday() == 6:
        _date = _date + timedelta(days=1)
        # print _date
    sql = """
        SELECT * FROM ((SELECT
                o.name "Order Number",
                o.date_order::date "Order Date",
                o.confirmation_date::date "Confirm Date",
                o.date_delivery "Forecasted Delivery Date",
                pp.default_code "Item Number",
                pt.name "Item Name",
                SUM(COALESCE(ol.product_uom_qty, 0)) "Qty Ordered",
                p.name "Agent",
                c.name "Customer",
                r.id "Route ID",
                SUM(COALESCE(ol.product_uom_qty, 0) * COALESCE(ol.price_unit, 0)) "Sale Value",
                pc.name "Category",
                psc.name "Sub Category",
                SUM(COALESCE(pt.weight, 0)) "Weight",
                r.name "Route",
                rpd.latitude "Geo Latitude",
                rpd.longitude "Geo Longitude",
                rgn.name "Depot",
                p.id "Agent ID",
                l.id "Location ID",
                l.name "Location"
            FROM sale_order o
            LEFT JOIN res_partner p
                ON o.partner_id = p.id
            LEFT JOIN res_users ru
                ON o.user_id = ru.id
            LEFT JOIN res_partner p2
                ON ru.partner_id = p2.id
            LEFT JOIN sale_order_line ol
                ON ol.order_id = o.id
            LEFT JOIN product_product pp
                ON pp.id = ol.product_id
            LEFT JOIN product_template pt
                ON pt.id = pp.product_tmpl_id
            LEFT JOIN product_category pc
                ON pc.id = pt.categ_id
            LEFT JOIN product_category psc
                ON psc.id = pc.parent_id
            LEFT JOIN res_partner_data rpd
                ON rpd.partner_id = p.id
            LEFT JOIN res_partner_location l
                ON l.id = rpd.location_id
            LEFT JOIN stock_warehouse s
                ON s.id = o.warehouse_id
            LEFT JOIN res_partner c
                ON c.id = o.customer_id
            LEFT JOIN delivery_route r
                ON r.id = rpd.route_id
            LEFT JOIN delivery_region rgn
                ON rgn.id = r.region_id
            WHERE o.state not in ('draft','cancel','rejected')
            AND o.date_delivery = '{}' and pp.default_code <> 'KX001'
            GROUP BY o.id, pp.id, pc.id, psc.id, p.id, r.id, rgn.id,
                l.id, s.id, c.id, pt.name, p2.name, rpd.latitude, rpd.longitude
            )

            UNION

            (select 
                o.name "Order Number",
                o.date_order::date "Order Date",
                o.confirmation_date::date "Confirm Date",
                o.date_delivery "Forecasted Delivery Date",
                pp.default_code "Item Number",
                pt.name "Item Name",
                SUM(COALESCE(ol.product_uom_qty, 0)) "Qty Ordered",
                p.name "Agent",
                c.name "Customer",
                r.id "Route ID",
                SUM(COALESCE(ol.product_uom_qty, 0) * COALESCE(ol.price_unit, 0)) "Sale Value",
                pc.name "Category",
                psc.name "Sub Category",
                SUM(COALESCE(pt.weight, 0)) "Weight",
                r.name "Route",
                rpd.latitude "Geo Latitude",
                rpd.longitude "Geo Longitude",
                rgn.name "Depot",
                p.id "Agent ID",
                l.id "Location ID",
                l.name "Location"
            from redelivery_log log
            JOIN stock_picking picking 
                ON picking.id = log.picking_id
            JOIN sale_order o 
                ON o.id = picking.sale_id
            LEFT JOIN res_partner p
                ON o.partner_id = p.id
            LEFT JOIN res_users ru
                ON o.user_id = ru.id
            LEFT JOIN res_partner p2
                ON ru.partner_id = p2.id
            LEFT JOIN sale_order_line ol
                ON ol.order_id = o.id
            LEFT JOIN product_product pp
                ON pp.id = ol.product_id
            LEFT JOIN product_template pt
                ON pt.id = pp.product_tmpl_id
            LEFT JOIN product_category pc
                ON pc.id = pt.categ_id
            LEFT JOIN product_category psc
                ON psc.id = pc.parent_id
            LEFT JOIN res_partner_data rpd
                ON rpd.partner_id = p.id
            LEFT JOIN res_partner_location l
                ON l.id = rpd.location_id
            LEFT JOIN stock_warehouse s
                ON s.id = o.warehouse_id
            LEFT JOIN res_partner c
                ON c.id = o.customer_id
            LEFT JOIN delivery_route r
                ON r.id = rpd.route_id
            LEFT JOIN delivery_region rgn
                ON rgn.id = r.region_id
            where log.new_date = '{}'
            GROUP BY o.id, pp.id, pc.id, psc.id, p.id, r.id, rgn.id,
                l.id, s.id, c.id, pt.name, p2.name, rpd.latitude, rpd.longitude
            )) as deliveries
            ORDER BY "Agent" ASC;
            """.format(_date.strftime('%Y-%m-%d'),_date.strftime('%Y-%m-%d'))

    utility.make_format_csv(
        [r for r in db.fetchall(sql)],
        "optiflow_redeliveries_report.csv",
        [
            "Order Number", "Order Date", "Confirm Date", "Forecasted Delivery Date",
            "Item Number", "Item Name", "Qty Ordered", "Agent", "Customer", "Route ID",
            "Sale Value", "Category", "Sub Category", "Weight", "Route", "Geo Latitude",
            "Geo Longitude", "Depot", "Agent ID", "Location ID", "Location"
        ]
    )
    utility.send_email("optiflow_redeliveries_report.csv", "grp_5")
    os.remove("optiflow_redeliveries_report.csv")

def product_price_report():
    sql = text("""
        WITH
            in_invoice AS (
                SELECT
                    DISTINCT ON(ail.product_id) ail.product_id, ail.uom_id, ail.price_unit
                FROM account_invoice_line ail
                JOIN account_invoice ai ON ai.id = ail.invoice_id
                JOIN product_product p ON p.id = ail.product_id
                JOIN product_template t ON t.id = p.product_tmpl_id
                WHERE ai.type = 'in_invoice' AND ai.state in ('open', 'paid')
                AND p.active IS TRUE AND t.sale_ok IS TRUE
                ORDER BY ail.product_id, ail.create_date DESC
            )
        SELECT
            pp.id "ID",
            pp.default_code "Code",
            COALESCE(pt.name, pt.name, '-') "Product Name",
            pc.name "Product Category",
            pt.list_price "Sale Price",
            pt.list_price "Cost Price",
            coalesce(i.price_unit, 0.00) "LICP (Total)",
            round(
                coalesce(i.price_unit/(1/u.factor), 0.00), 2
            ) "LICP (per Unit)",
            case
                when u.name is null then '-'
                else u.name
            end "UoM (Invoice)"
        FROM
            product_product pp
        JOIN product_template pt ON pt.id = pp.product_tmpl_id
        JOIN product_category pc ON pc.id = pt.categ_id
        LEFT JOIN in_invoice i ON i.product_id = pp.id
        LEFT JOIN product_uom u ON u.id = i.uom_id
        WHERE coalesce(i.price_unit/(1/u.factor), 0.00) > pt.list_price
        AND pp.active IS TRUE AND pt.sale_ok IS TRUE
        ORDER BY pp.default_code ASC;
    """)

    utility.make_format_csv(
        [r for r in database.pull_conn.execute(sql)],
        "potential_loss_makers.csv",
        [
            "ID", "Code", "Product Name", "Product Category", "Sale Price", "Cost Price",
            "Last Invoice Price (Total)", "Last Invoice Price (per Unit)",
            "Invoice UoM"
        ]
    )
    utility.send_email("potential_loss_makers.csv", "grp_3")
    os.remove("potential_loss_makers.csv")


def potential_margins_report(qry_date=False):
    _date = (
        datetime.strptime(
            qry_date or date.today().__str__(), "%Y-%m-%d"
        ) - timedelta(days=1)
    )

    if _date.weekday() == 6:
        _date = _date - timedelta(days=1)

    sql = text("""
        WITH
            s_invoice AS (
                SELECT
                    DISTINCT ON(ail.product_id) ail.product_id, ail.uos_id, ail.price_unit, ai.date_invoice
                FROM account_invoice_line ail
                JOIN account_invoice ai ON ai.id = ail.invoice_id
                JOIN product_product p ON p.id = ail.product_id
                JOIN product_template t ON t.id = p.product_tmpl_id
                WHERE ai.type = 'in_invoice' AND ai.state in ('open', 'paid')
                AND p.active IS TRUE AND t.sale_ok IS TRUE
                ORDER BY ail.product_id, ail.create_date DESC
            ),
            c_invoice AS (
                SELECT
                    DISTINCT ON(ail.product_id) ail.product_id, ail.uos_id, ail.price_unit, ai.date_invoice
                FROM account_invoice_line ail
                JOIN account_invoice ai ON ai.id = ail.invoice_id
                JOIN product_product p ON p.id = ail.product_id
                JOIN product_template t ON t.id = p.product_tmpl_id
                WHERE ai.type = 'out_invoice' AND ai.state in ('open', 'paid')
                AND p.active IS TRUE AND t.sale_ok IS TRUE
                ORDER BY ail.product_id, ail.create_date DESC
            )
        SELECT
            DISTINCT ON(pp.default_code) pp.id "ID",
            pp.default_code "Code",
            COALESCE(pt.name, pp.name, '-') "Product Name",
            pc.name "Product Category" ,
            pt.list_price "Sale Price",
            pt.standard_price "Cost Price",
            coalesce(i.price_unit, 0.00) "Purchase LICP (Total)",
            round(
                coalesce(i.price_unit/(1/u.factor), 0.00), 2
            ) "Purchase LICP (per Unit)",
            i.date_invoice "Purchase Date",
            case
                when u.name is null then '-'
                else u.name
            end "UoM (Purchase Invoice)",
            coalesce(j.price_unit, 0.00) "Sale LICP (Total)",
            round(
                coalesce(j.price_unit/(1/v.factor), 0.00), 2
            ) "Sale LICP (per Unit)",
            j.date_invoice "Sale Date",
            case
                when v.name is null then '-'
                else v.name
            end "UoM (Sale Invoice)"
        FROM
            product_product pp
        JOIN product_template pt ON pt.id = pp.product_tmpl_id
        JOIN product_category pc ON pc.id = pt.categ_id
        JOIN sale_order_line l ON l.product_id = pp.id
        JOIN sale_order o ON o.id = l.order_id
        LEFT JOIN s_invoice i ON i.product_id = pp.id
        LEFT JOIN product_uom u ON u.id = i.uos_id
        LEFT JOIN c_invoice j ON j.product_id = pp.id
        LEFT JOIN product_uom v ON v.id = j.uos_id
        WHERE o.state IN ('progress', 'done') AND o.date_confirm = '%s'
        AND pp.active IS TRUE AND pt.sale_ok IS TRUE
        ORDER BY pp.default_code ASC;
    """ % (_date.__str__()))

    utility.make_format_csv(
        [r for r in database.pull_conn.execute(sql)],
        "potential_margins_report.csv",
        [
            "ID", "Code", "Product Name", "Product Category", "Sale Price", "Cost Price",
            "Purchase - Last Invoice Price (Total)", "Purchase - Last Invoice Price (per Unit)",
            "Purchase Date", "Purchase Invoice UoM", "Sale - Last Invoice Price (Total)",
            "Sale - Last Invoice Price (per Unit)", "Sale Date", "Sale Invoice UoM"
        ]
    )
    utility.send_email("potential_margins_report.csv", "grp_4")
    os.remove("potential_margins_report.csv")


if __name__ == "__main__":
    if sys.argv[1] == "today_confirmed_order_lines":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        today_confirmed_order_lines(qry_date)
    elif sys.argv[1] == "requisition_vs_purchase":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        requisition_vs_purchase(qry_date)
    elif sys.argv[1] == "purchase_planned_inbound":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        purchase_planned_inbound(qry_date)
    elif sys.argv[1] == "available_qty_outbound":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        available_qty_outbound(qry_date)
    elif sys.argv[1] == "delivery_manifest":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        delivery_manifest(qry_date)
    elif sys.argv[1] == "delivery_returns":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        delivery_returns(qry_date)
    elif sys.argv[1] == "historical_qoh":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        historical_qoh(qry_date)
    elif sys.argv[1] == "daily_layaway_order":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        daily_layaway_orders(qry_date)
    elif sys.argv[1] == "logistics_optiflow_report":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = False

        logistics_optiflow_report(qry_date)
    elif "optiflow_redeliveries_report":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = False
        optiflow_redeliveries_report(qry_date)

    elif sys.argv[1] == "product_price_report":
        product_price_report()
    elif sys.argv[1] == "potential_margins_report":
        if sys.argv.__len__() == 3:
            qry_date = sys.argv[2]
        else:
            qry_date = date.today().__str__()

        potential_margins_report(qry_date)
    else:
        pass
