"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    BROKER_URL = "PLAINTEXT://localhost:9092"
    SCHEMA_REGISTRY_UTL = "http://localhost:8081"

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
            list_topics=[],
            client=AdminClient({"bootstrap.servers": BROKER_URL, })
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.list_topics = list_topics
        self.client = client
        self.broker_properties = {
            "bootstrap.servers": Producer.BROKER_URL,
            "linger.ms": "500",
        }

        self.list_topics = client.list_topics(timeout=5).topics
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=CachedSchemaRegistryClient(Producer.SCHEMA_REGISTRY_UTL),
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        try:
            if self.list_topics.get(self.topic_name):
                logger.info("topic already exists not creating %s", self.topic_name)
            else:
                logger.info(f"creating topic: {self.topic_name}")
                futures = self.client.create_topics(
                    [
                        NewTopic(
                            topic=self.topic_name,
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
                for future in futures.items():
                    try:
                        future
                        logger.info(f"topic created: {self.topic_name}")
                    except Exception as e:
                        logger.info(f"failed to create topic {self.topic_name}: {e}")
                        raise
        except Exception as e:
            logger.info(f"failed to get metadata for topic {self.topic_name}: {e}")
            raise

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is not None:
            logger.debug("closing producer")
            self.producer.flush()
