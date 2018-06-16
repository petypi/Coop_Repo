from lib.sale_order import sale_order
import logging

def process():
    SO = sale_order()
    synced = SO.send_balance_sms()
    print("We've sent {:,} balance SMS to agents.".format(synced.__len__()))

if __name__ == "__main__":
    process()