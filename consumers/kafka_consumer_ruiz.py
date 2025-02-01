#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
from collections import Counter

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Define a function to process messages
#####################################

word_counter = Counter()
ALERT_THRESHOLD = 5  # Set threshold for triggering alerts
ALERT_WORD = "CRITICAL"  # Define word that triggers alerts

def process_message(message: str) -> None:
    """
    Process a single message.

    - Logs the message.
    - Tracks word frequency.
    - Triggers an alert if a word exceeds the threshold.
    
    Args:
        message (str): The message to process.
    """
    logger.info(f"Processing message: {message}")
    
    # Word frequency tracking
    words = message.split()
    word_counter.update(words)
    logger.info(f"Updated word count: {dict(word_counter)}")
    
    # Custom alert logic
    if word_counter[ALERT_WORD] >= ALERT_THRESHOLD:
        print(f"ALERT: The word '{ALERT_WORD}' has appeared {word_counter[ALERT_WORD]} times!")
        logger.warning(f"ALERT: The word '{ALERT_WORD}' has appeared {word_counter[ALERT_WORD]} times!")
    
    # An alert for special conditions
    if "Kafka!" in message:
        print(f"ALERT: The special message was found! \n{message}")
        logger.warning(f"ALERT: The special message was found! \n{message}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
