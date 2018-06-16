from lib.Orders import Orders, DB_SETTINGS
from lib.Postgres import Postgres
import os

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    Orders = Orders(CONN)
    ORDERS = Orders.pending_orders_report()
    Orders.format_xlsx(ORDERS)
    Orders.email_results('pending_orders_report.xlsx')
    os.remove('pending_orders_report.xlsx')
