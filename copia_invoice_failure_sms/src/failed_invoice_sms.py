from lib.Deliveries import Deliveries, DB_SETTINGS
from lib.Postgres import Postgres
import sys


def process():
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    DELIVERIES = Deliveries(CONN)
    ORDERS = DELIVERIES.get_unsynced_deliveries()
    if not ORDERS:
        return True
    print len(ORDERS)
    print "Are orders more than %s" % sys.argv[1]
    if len(ORDERS) > (int(sys.argv[1]) or 400):
        DELIVERIES.sms_results(len(ORDERS))


if __name__ == "__main__":
    process()
