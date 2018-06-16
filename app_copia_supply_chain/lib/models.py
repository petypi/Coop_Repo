# Models

from db import DB
from sqlalchemy import Table, Column, Integer, String, Date, Float, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base

database = DB()
Base = declarative_base()

# Push
class ConfirmedSaleOrder(Base):
    __tablename__ = "t_sc_confirmed_sale_order"

    id = Column(Integer, primary_key=True)
    so_id = Column(Integer)
    so_name = Column(String)
    so_state = Column(String)
    date_order = Column(Date)
    date_delivery = Column(Date)
    product_id = Column(Integer)
    product_code = Column(String)
    product_name = Column(String)
    qty_ordered = Column(Float)
    agent_id = Column(Integer)
    agent_name = Column(String)
    customer_id = Column(Integer)
    customer_name = Column(String)
    route_id = Column(Integer)
    route_name = Column(String)
    sol_id = Column(Integer)
    sol_value = Column(Float)
    date_report = Column(Date)


class RequisitionVsPurchase(Base):
    __tablename__ = "t_sc_requisition_vs_purchase"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    product_code = Column(String)
    product_name = Column(String)
    product_categ_name = Column(String)
    can_stock = Column(Boolean)
    qoh = Column(Float)
    uom_name = Column(String)
    qty_ordered = Column(Float)
    pending_out_qty = Column(Float)
    pending_in_qty = Column(Float)
    qty_2_purchase = Column(Float)
    qty_purchased = Column(Float)
    state = Column(String)
    date_report = Column(Date)


class PurchaseVsPlannedInbound(Base):
    __tablename__ = "t_sc_purchase_planned_inbound"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(Integer)
    supplier_name = Column(String)
    po_id = Column(Integer)
    po_name = Column(String)
    product_id = Column(Integer)
    product_code = Column(String)
    product_name = Column(String)
    qty_purchased = Column(Float)
    qty_received = Column(Float)
    state = Column(String)
    date_report = Column(Date)


class AvailableQtyVsOutbound(Base):
    __tablename__ = "t_sc_available_qty_outbound"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    product_code = Column(String)
    product_name = Column(String)
    qoh = Column(Float)
    qty_inbound = Column(Float)
    qty_available = Column(Float)
    qty_outbound = Column(Float)
    date_delivery = Column(Date)
    date_report = Column(Date)


class DeliveryManifest(Base):
    __tablename__ = "t_sc_delivery_manifest"

    id = Column(Integer, primary_key=True)
    so_id = Column(Integer)
    so_name = Column(String)
    so_state = Column(String)
    date_order = Column(Date)
    date_confirm = Column(Date)
    date_delivery = Column(Date)
    agent_id = Column(Integer)
    agent_name = Column(String)
    route_id = Column(Integer)
    route_name = Column(String)
    invoice_id = Column(Integer)
    invoice_number = Column(String)
    receipt_ref = Column(String)
    invoice_state = Column(String)
    date_invoice = Column(Date)
    product_id = Column(Integer)
    product_code = Column(String)
    product_name = Column(String)
    qty_invoice = Column(Float)
    date_report = Column(Date)


class DeliveryReturns(Base):
    __tablename__ = "t_sc_delivery_returns"

    id = Column(Integer, primary_key=True)
    so_id = Column(Integer)
    so_name = Column(String)
    agent_id = Column(Integer)
    agent_name = Column(String)
    return_no = Column(String)
    product_id = Column(Integer)
    product_code = Column(String)
    product_name = Column(String)
    product_returned_quantity = Column(Float)
    product_return_value = Column(Float)
    claim_origine = Column(String)
    date_create = Column(DateTime)
    date_report = Column(Date)


class HistoricalQoH(Base):
    __tablename__ = "t_sc_historical_qoh"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    product_code = Column(String)
    product_name = Column(String)
    qty_on_hand = Column(Float)
    qty_ordered = Column(Float)
    avg_4_wk_sale = Column(Float)
    date_report = Column(Date)

Base.metadata.create_all(database.push_conn)
