import logging
import sys
import time
from threading import Thread
from tabulate import tabulate

from kafka import KafkaConsumer
from google.oauth2 import service_account
from google.cloud import bigquery



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
        self.consumer = KafkaConsumer(bootstrap_servers='34.44.153.3:9092',  # use your VM's external IP Here!
                                      auto_offset_reset='earliest',
                                      consumer_timeout_ms=10000)

        self.consumer.subscribe(topics=[topic])
        self.time_between_reads = time_between_reads
        self.header = header
        credentials = service_account.Credentials.from_service_account_file('stream-processing/kafka-consumer/service_worker.json')
        self.client = bigquery.Client(credentials=credentials)

    def update_table(self, data):
        for service, value in data:
            print(f"Updating {service} with {value} in {self.topic}")
            stmt = f"""
                UPDATE Assignment_Data.{self.topic}
                SET amount_movies = {value}
                WHERE service = '{service}';
            """

        query_job = self.client.query(stmt)  # API request
        query_job.result()

    def read_from_topic(self):
        data = []
        for msg in self.consumer:
            if msg.key:
                data.append([msg.key.decode('utf-8'), msg.value.decode('utf-8')])
            else:
                data.append([msg.value.decode('utf-8')])

        if len(data) > 0:
            self.update_table(data)
            print(tabulate(data, headers=["Service", self.header]) + "\n")	

    def check_table(self):
        stmt = f"""
            SELECT * FROM Assignment_Data.{self.topic}
            WHERE service = 'Netflix' OR service = 'Disney Plus' OR service = 'Amazon Prime';
        """

        query_job = self.client.query(stmt)  # API request
        query_job.result()

        # Check if "Netflix", "Disney Plus" and "Amazon Prime" are in the table
        if len(list(query_job)) < 3:
            stmt = f"""
                INSERT Assignment_Data.{self.topic}
                (service, amount_movies)
                VALUES ('Netflix', 0), ('Disney Plus', 0), ('Amazon Prime', 0);
            """

            query_job = self.client.query(stmt)

    def run(self):
        self.check_table()

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