from lib.SupplyChain import SupplyChain, DB_SETTINGS
from lib.Postgres import Postgres
import os

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    RPT = SupplyChain(CONN)
    avo = RPT.available_qty_vs_outbound()
    RPT.format_csv(avo, 'available_stock_vs_outbound.csv', [
        'Code', 'Product Name',
        'QoH at Confirmation', 'Day\'s Inbounds', 'Available Stock',
        'Outbound', 'Outbound Delivery Date', 'Report Date'
    ])
    RPT.email_results('available_stock_vs_outbound.csv')
    os.remove('available_stock_vs_outbound.csv')
