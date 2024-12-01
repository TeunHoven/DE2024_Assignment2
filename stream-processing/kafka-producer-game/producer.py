import csv
import json
import threading
import time

from kafka import KafkaProducer


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


def produce_from_file(producer, file, service):
    print(file)
    f = open(file, encoding='utf-8')
    reader = csv.DictReader(f)

    for line in reader:
        line["service"] = service
        line["timestamp_in_ms"] = int(time.time() * 1000)
        kafka_python_producer_sync(producer, json.dumps(line), "scraper")
    f.close()


def run_job():
    producer = KafkaProducer(bootstrap_servers='34.72.19.22:9092')  # use your VM's external IP Here!
    # Change the path to your laptop!
    # if you want to learn about threading in python, check the following article
    # https://realpython.com/intro-to-python-threading/
    # if you want to schedule a job https://www.geeksforgeeks.org/python-schedule-library/
    t1 = threading.Thread(target=produce_from_file,
                          args=(producer, 'C:/Users/TeunH/Documents/Universiteit/DBSE/Semester 1/Data Engineering/Assignment 2/DE2024_Assignment2/data/netflix_data.csv', 'Netflix'))
    t2 = threading.Thread(target=produce_from_file,
                          args=(producer, 'C:/Users/TeunH/Documents/Universiteit/DBSE/Semester 1/Data Engineering/Assignment 2/DE2024_Assignment2/data/amazon_prime_data.csv', 'Amazon Prime'))
    t3 = threading.Thread(target=produce_from_file,
                          args=(producer, 'C:/Users/TeunH/Documents/Universiteit/DBSE/Semester 1/Data Engineering/Assignment 2/DE2024_Assignment2/data/disney_plus_data.csv', 'Disney Plus'))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()


if __name__ == '__main__':
    run_job()
