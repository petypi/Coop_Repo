from lib.FarmingConstruction import FarmingConstruction, DB_SETTINGS
from lib.Postgres import Postgres
import os

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    ORDERS = FarmingConstruction(CONN)
    TODAYS_ORDERS = ORDERS.get_todays_orders()
    ORDERS.format_xlsx(TODAYS_ORDERS)
    ORDERS.email_results('FarmingConstructionOrders.xlsx')
    os.remove('FarmingConstructionOrders.xlsx')
