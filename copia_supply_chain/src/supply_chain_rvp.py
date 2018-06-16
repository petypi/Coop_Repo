from lib.SupplyChain import SupplyChain, DB_SETTINGS
from lib.Postgres import Postgres
import os

if __name__ == '__main__':
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    RPT = SupplyChain(CONN)
    rvp = RPT.requisitions_vs_purchases()
    RPT.format_csv(rvp, 'requisitions_vs_purchases.csv', [
        'Code', 'Product', 'Category', 'Stockable', 'QoH', 'UoM', 'Qty Ordered',
        'Pending Outgoing Qty', 'Pending Incoming Shipments',
        'Qty to Purchase', 'Qty Purchased', 'Date', 'State'
    ])
    RPT.email_results('requisitions_vs_purchases.csv')
    os.remove('requisitions_vs_purchases.csv')
