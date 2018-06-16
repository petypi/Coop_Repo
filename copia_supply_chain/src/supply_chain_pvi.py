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
    pvi = RPT.purchases_vs_planned_inbounds()
    RPT.format_csv(pvi, 'purchases_vs_planned_inbounds.csv', [
        'Code', 'Product Name',
        'Qty Purchased', 'Qty Planned Inbound', 'Status'
    ])
    RPT.email_results('purchases_vs_planned_inbounds.csv')
    os.remove('purchases_vs_planned_inbounds.csv')
