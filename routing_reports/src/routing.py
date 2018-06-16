from lib.Routing import Routing, DB_SETTINGS
from lib.Postgres import Postgres
from datetime import datetime, timedelta
import os

if __name__ == "__main__":
    CONN = Postgres(DB_SETTINGS['host'],
                    DB_SETTINGS['port'],
                    DB_SETTINGS['username'],
                    DB_SETTINGS['password'],
                    DB_SETTINGS['db_name'],
                    DB_SETTINGS['timeout'])
    BALANCER = Routing(CONN)                                               
    delivery_date = datetime.strftime(datetime.today() + timedelta(days=1),
                                      '%Y-%m-%d')
    if datetime.weekday(datetime.now()) == 5:
                delivery_date = datetime.strftime(datetime.today() +
                                                  timedelta(days=2), 
                                                  '%Y-%m-%d')
                print delivery_date
    deliveries = BALANCER.get_deliveries(delivery_date)               
    BALANCER.format_xlsx(deliveries)                                            
    BALANCER.email_results('routing_report.xlsx') 
    os.remove('routing_report.xlsx')
