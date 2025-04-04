from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_topics():
    """
    Create Kafka topics for different data streams
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id="fantasy-football-admin"
    )
    
    # Define topic names
    topic_list = [
        NewTopic(name="player-stats", num_partitions=3, replication_factor=1),
        NewTopic(name="game-events", num_partitions=3, replication_factor=1),
        NewTopic(name="player-injuries", num_partitions=2, replication_factor=1),
        NewTopic(name="fantasy-transactions", num_partitions=3, replication_factor=1),
        NewTopic(name="projections-updates", num_partitions=2, replication_factor=1),
    ]
    
    # Create topics
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info("Topics created successfully")
    except TopicAlreadyExistsError:
        logger.warning("Topics already exist")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_kafka_topics()