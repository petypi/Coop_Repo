from lib.Orderconfirm import Orderconfirm, DB_SETTINGS
from lib.Postgres import Postgres
import os
from datetime import datetime, timedelta
import sys
if __name__ == "__main__":
    date_confirm = datetime.today().date()
    #skip_dates = ['2017-10-24','2017-10-25']
    #if str(date_confirm) in skip_dates:
    #    sys.exit()
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    ORDERS = Orderconfirm(CONN)
    ORDER = ORDERS.confirm_orders()
    if ORDER:
        ORDERS.email_results()
