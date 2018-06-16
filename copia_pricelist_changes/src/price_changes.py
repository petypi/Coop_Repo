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
    pricelist_change = CHANGES.get_pricelist_changes()
    CHANGES.format_xlsx(pricelist_change)
    CHANGES.email_results('pricelist_changes.xlsx')
    os.remove('pricelist_changes.xlsx')
