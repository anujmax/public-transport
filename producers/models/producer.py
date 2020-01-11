"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
            bootstrap_servers="PLAINTEXT://localhost:9092",
            schema_registry_url="http://localhost:8081"
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": "500",
            # TODO
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=CachedSchemaRegistryClient(self.schema_registry_url),
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(self.broker_properties)
        for topic in self.existing_topics:
            topic_metadata = client.list_topics(timeout=5)
            if topic_metadata.topics.get(topic):
                logger.info("topic already exists not creating.%s", topic)
            else:
                futures = client.create_topics(
                    [
                        NewTopic(
                            topic=topic,
                            num_partitions=self.num_partitions,
                            replication_factor=self.num_replicas,
                            config={
                                "cleanup.policy": "compact",
                                "compression.type": "lz4",
                                "delete.retention.ms": "100",
                                "file.delete.delay.ms": "100"
                            }
                        )
                    ]
                )

        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        # logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
