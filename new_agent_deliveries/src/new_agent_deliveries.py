from lib.Deliveries import Deliveries, DB_SETTINGS
from lib.Postgres import Postgres
import os

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    DELIVERIES = Deliveries(CONN)
    ORDERS = DELIVERIES.get_new_agent_deliveries()
    DELIVERIES.format_xlsx(ORDERS)
    DELIVERIES.email_results('new_deliveries.xlsx')
    os.remove('new_deliveries.xlsx')
