import logging
import sys
import time
from threading import Thread
from tabulate import tabulate

from kafka import KafkaConsumer


# if you want to learn about threading in python, check the following article
# https://realpython.com/intro-to-python-threading/

def configure_logger():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)


class KafkaMessageConsumer(Thread):

    def __init__(self, topic, time_between_reads=30, header="value"):
        Thread.__init__(self)
        self.topic = topic
        self.consumer = KafkaConsumer(bootstrap_servers='34.16.122.232:9092',  # use your VM's external IP Here!
                                      auto_offset_reset='earliest',
                                      consumer_timeout_ms=10000)

        self.consumer.subscribe(topics=[topic])
        self.time_between_reads = time_between_reads
        self.header = header

    def read_from_topic(self):
        data = []
        for msg in self.consumer:
            if msg.key:
                data.append([msg.key.decode('utf-8'), msg.value.decode('utf-8')])
            else:
                data.append([msg.value.decode('utf-8')])

        print(tabulate(data, headers=["Service", self.header]) + "\n")	

    def run(self):
        while True:
            try:
                self.read_from_topic()
                time.sleep(self.time_between_reads)
            except Exception as err:
                logging.info(f"Unexpected {err=}, {type(err)=}")
                time.sleep(self.time_between_reads)


if __name__ == '__main__':
    configure_logger()

    c1 = KafkaMessageConsumer('top_movies', 30, 'Amount Top Movies (Score: 9+)')
    c2 = KafkaMessageConsumer('subtop_movies', 30, 'Amount Sub Top Movies (Score: 8-9)')

    c1.start()
    c2.start()

    c1.join()
    c2.join()