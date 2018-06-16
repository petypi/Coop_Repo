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
    rvp = RPT.delivery_manifest()
    RPT.format_csv(rvp, 'delivery_manifest.csv', [
        'Order Name', 'Order State', 'Order Date', 'Order Confirmation',
        'Delivery Date', 'Agent Name', 'Route Name', 'Invoice',
        'Receipt Ref#','Invoice State','Product Code,' 'Product','Quantity'
    ])
    RPT.email_results('delivery_manifest.csv')
    os.remove('delivery_manifest.csv')
