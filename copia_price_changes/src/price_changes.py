from lib.Changes import Changes, DB_SETTINGS
from lib.Postgres import Postgres
import os

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    CHANGES = Changes(CONN)
    PRODUCTS = CHANGES.get_price_changes()
    CHANGES.format_xlsx(PRODUCTS)
    CHANGES.email_results('price_changes.xlsx')
    os.remove('price_changes.xlsx')
