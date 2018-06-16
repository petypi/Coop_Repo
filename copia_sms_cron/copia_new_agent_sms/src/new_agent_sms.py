from lib.sale_order import sale_order
import logging


def process():
    SO = sale_order()
    synced = SO.send_agent_sms()
    print("We sent {:,} sent SMS to new agents.".format(synced.__len__()))

if __name__ == "__main__":
    process()