from lib.ReplenishProducts import ReplenishProducts, DB_SETTINGS
from lib.Postgres import Postgres
import os

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    PRODUCTS = ReplenishProducts(CONN)
    GOT_PRODUCTS = PRODUCTS.get_products()
    PRODUCTS.format_xlsx(GOT_PRODUCTS)
    PRODUCTS.email_results('ReplenishProductsScheduleReport.xlsx')
    os.remove('ReplenishProductsScheduleReport.xlsx')
