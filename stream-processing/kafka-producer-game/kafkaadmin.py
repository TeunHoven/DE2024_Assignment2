from kafka.admin import KafkaAdminClient, NewTopic


def delete_topics(admin):
    admin.delete_topics(topics=['scraper'])
    admin.delete_topics(topics=['top_movies'])
    admin.delete_topics(topics=['subtop_movies'])


def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)


if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="34.16.71.83:9092",
                                    client_id='Assignment2')  # use your VM's external IP Here!
    topic_list = [NewTopic(name="scraper", num_partitions=1, replication_factor=1),
                  NewTopic(name="top_movies", num_partitions=1, replication_factor=1),
                  NewTopic(name="subtop_movies", num_partitions=1, replication_factor=1)]
    create_topics(admin_client, topic_list)
    #delete_topics(admin_client)