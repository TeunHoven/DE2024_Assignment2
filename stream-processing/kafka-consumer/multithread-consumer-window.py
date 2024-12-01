import logging
import sys
import time
from threading import Thread
from tabulate import tabulate

from kafka import KafkaConsumer
from google.oauth2 import service_account
from google.cloud import bigquery

from datetime import datetime
from uuid import uuid4


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
        self.consumer = KafkaConsumer(bootstrap_servers='34.72.19.22:9092',  # use your VM's external IP Here!
                                      auto_offset_reset='earliest',
                                      consumer_timeout_ms=10000)

        self.consumer.subscribe(topics=[topic])
        self.time_between_reads = time_between_reads
        self.header = header
        self.credentials = service_account.Credentials.from_service_account_file('stream-processing/kafka-consumer/service_worker.json')
        self.client = bigquery.Client(credentials=self.credentials)

    def get_window_id(self, window_start, window_length):
        stmt = f"""
            SELECT window_id
            FROM Information.window
            WHERE window_start = '{window_start}'
            LIMIT 1;
        """

        query_job = self.client.query(stmt)
        res = list(query_job.result())

        # If the window is not in the database, add it
        if len(res) == 0:
            window_id = uuid4()

            stmt = f"""
                INSERT Information.window
                (window_id, window_start, window_length)
                VALUES ('{window_id}', '{window_start}', {window_length});
            """


            temp_client = bigquery.Client(credentials=self.credentials)

            query_job = temp_client.query(stmt)
            query_job.result()

            return window_id
        else:
            return list(res)[0][0]

    def get_service_id(self, service):
        stmt = f"""
            SELECT service_id
            FROM Information.service
            WHERE service_name = '{service}'
            LIMIT 1;
        """

        query_job = self.client.query(stmt)
        res = list(query_job.result())

        # If the service is not in the database, add it
        if len(res) == 0:
            return None
        else:
            return list(res)[0][0]

    def update_table(self, data):
        all_values = []

        for key, value in data:
            splitted_key = key.split(";")

            if len(splitted_key) != 3:
                logging.error(f"Key {key} is not in the right format")
                continue
            window_start = splitted_key[0]
            window_length = splitted_key[1]
            service = splitted_key[2]

            window_start = datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S")

            window_id = self.get_window_id(window_start, window_length)
            service_id = self.get_service_id(service)

            value = int(value)

            value = f"('{window_id}', '{service_id}', {value})"
            all_values.append(value)

        stmt = f"""
            INSERT Assignment_Data.{self.topic}
            (window_id, service_id, amount_movies)
            VALUES {",".join(all_values)};
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