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
    ORDERS = Orders.get_new_agent_orders()
    PRODUCT = Orders.get_high_value_product()
    Orders.format_xlsx(ORDERS, PRODUCT, 'draft_high_value_orders.xlsx')
    Orders.email_results('draft_high_value_orders.xlsx')
    os.remove('draft_high_value_orders.xlsx')
