from lib.Institutions import Institutions, DB_SETTINGS
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
    BALANCER = Institutions(CONN)                                               
    delivery_date1 = datetime.today() + timedelta(days=1)                      
    delivery_date2 = datetime.today() + timedelta(days=2)                      
    deliveries = BALANCER.get_institution_deliveries(delivery_date1)               
    deliveries2 = BALANCER.get_institution_deliveries(delivery_date2)           
    null_return = [deliveries.append(x) for x in deliveries2]                   
    BALANCER.format_xlsx(deliveries)                                            
    BALANCER.email_results('expected_deliveries.xlsx') 
    os.remove('expected_deliveries.xlsx')
