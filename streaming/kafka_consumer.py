#!/usr/bin/env python
import json
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values
import os
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', 5432),
    'database': os.environ.get('DB_NAME', 'fantasy_football'),
    'user': os.environ.get('DB_USER', 'fantasy'),
    'password': os.environ.get('DB_PASSWORD', 'fantasy123')
}

# Kafka connection parameters
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'fantasy-football-consumer')

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    return psycopg2.connect(**DB_PARAMS)

class PlayerStatsConsumer(threading.Thread):
    """Consumer for player stats topic"""
    
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            'player-stats',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def stop(self):
        """Stop the consumer thread"""
        self.stop_event.set()
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting player stats consumer")
        
        while not self.stop_event.is_set():
            for message in self.consumer:
                if self.stop_event.is_set():
                    break
                
                try:
                    # Process the message
                    self.process_player_stats(message.value)
                except Exception as e:
                    logger.error(f"Error processing player stats message: {e}")
            
            time.sleep(0.1)  # Small sleep to prevent CPU hogging
        
        # Close the consumer
        self.consumer.close()
        logger.info("Player stats consumer stopped")
    
    def process_player_stats(self, message):
        """Process player stats messages"""
        source = message.get('source')
        msg_type = message.get('type')
        timestamp = message.get('timestamp')
        data = message.get('data', [])
        
        logger.info(f"Processing player stats message from {source}: {msg_type} with {len(data)} records")
        
        if not data:
            logger.warning("No data in message")
            return
        
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Process based on message type and source
            if source == 'espn' and msg_type == 'players':
                self._process_espn_players(cursor, data)
            elif source == 'nfl' and msg_type == 'player_stats':
                self._process_nfl_stats(cursor, data)
            elif source == 'sleeper' and msg_type == 'players':
                self._process_sleeper_players(cursor, data)
            elif source == 'pfr' and msg_type in ['passing', 'rushing', 'receiving', 'gamelog']:
                self._process_pfr_stats(cursor, data, msg_type)
            else:
                logger.warning(f"Unknown message type: {source}/{msg_type}")
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error processing player stats: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def _process_espn_players(self, cursor, data):
        """Process ESPN player data"""
        # In a real implementation, this would handle ESPN-specific data structures
        logger.info("Processing ESPN player data")
        
        # For demonstration, we'll just log the first player
        if data:
            logger.info(f"First player: {data[0]}")
    
    def _process_nfl_stats(self, cursor, data):
        """Process NFL player stats"""
        # In a real implementation, this would handle NFL-specific data structures
        logger.info("Processing NFL player stats")
        
        # For demonstration, we'll just log some info
        if data:
            logger.info(f"Stats for season: {data.get('season')}, week: {data.get('week')}")
    
    def _process_sleeper_players(self, cursor, data):
        """Process Sleeper player data"""
        # In a real implementation, this would handle Sleeper-specific data structures
        logger.info("Processing Sleeper player data")
        
        # For demonstration, we'll just log the count
        logger.info(f"Received {len(data)} players from Sleeper")
    
    def _process_pfr_stats(self, cursor, data, stat_type):
        """Process Pro Football Reference stats"""
        # In a real implementation, this would handle PFR-specific data structures
        logger.info(f"Processing PFR {stat_type} stats")
        
        # For demonstration, we'll just log the count
        logger.info(f"Received {len(data)} {stat_type} records from PFR")

class PlayerInjuriesConsumer(threading.Thread):
    """Consumer for player injuries topic"""
    
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            'player-injuries',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def stop(self):
        """Stop the consumer thread"""
        self.stop_event.set()
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting player injuries consumer")
        
        while not self.stop_event.is_set():
            for message in self.consumer:
                if self.stop_event.is_set():
                    break
                
                try:
                    # Process the message
                    self.process_injury(message.value)
                except Exception as e:
                    logger.error(f"Error processing injury message: {e}")
            
            time.sleep(0.1)  # Small sleep to prevent CPU hogging
        
        # Close the consumer
        self.consumer.close()
        logger.info("Player injuries consumer stopped")
    
    def process_injury(self, message):
        """Process injury messages"""
        source = message.get('source')
        msg_type = message.get('type')
        timestamp = message.get('timestamp')
        data = message.get('data', {})
        
        logger.info(f"Processing injury message from {source}: {msg_type}")
        
        if not data:
            logger.warning("No data in message")
            return
        
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # For demonstration purposes, we'll use a simplified approach
            # In a real system, you would extract and transform the injury data 
            # based on the source's specific format
            
            # Get the current date
            today = datetime.now().date()
            
            # Sample injury data processing
            if source == 'sleeper' and msg_type == 'injuries':
                for player_id, injury_info in data.items():
                    if not injury_info:
                        continue
                        
                    # Check if player exists
                    cursor.execute("SELECT 1 FROM players WHERE source_id = %s", (player_id,))
                    if not cursor.fetchone():
                        logger.warning(f"Player with source_id {player_id} not found")
                        continue
                    
                    # Extract injury details
                    injury_type = injury_info.get('injury_type', 'Unknown')
                    body_part = injury_info.get('body_part', 'Unknown')
                    status = injury_info.get('status', 'Unknown')
                    
                    # Insert into injuries table
                    cursor.execute("""
                    INSERT INTO injuries (
                        player_id, report_date, injury_type, body_part,
                        practice_status, game_status, notes
                    )
                    VALUES (
                        (SELECT player_id FROM players WHERE source_id = %s),
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (player_id, report_date) DO UPDATE SET
                        practice_status = EXCLUDED.practice_status,
                        game_status = EXCLUDED.game_status,
                        updated_at = CURRENT_TIMESTAMP
                    """, (
                        player_id, today, injury_type, body_part,
                        injury_info.get('practice_status'),
                        status,
                        injury_info.get('notes')
                    ))
            
            conn.commit()
            logger.info("Processed injury data successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error processing injuries: {e}")
        finally:
            cursor.close()
            conn.close()

class GameEventsConsumer(threading.Thread):
    """Consumer for game events topic"""
    
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            'game-events',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def stop(self):
        """Stop the consumer thread"""
        self.stop_event.set()
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting game events consumer")
        
        while not self.stop_event.is_set():
            for message in self.consumer:
                if self.stop_event.is_set():
                    break
                
                try:
                    # Process the message
                    self.process_game_event(message.value)
                except Exception as e:
                    logger.error(f"Error processing game event: {e}")
            
            time.sleep(0.1)  # Small sleep to prevent CPU hogging
        
        # Close the consumer
        self.consumer.close()
        logger.info("Game events consumer stopped")
    
    def process_game_event(self, message):
        """Process game event messages"""
        event_type = message.get('event_type')
        game_id = message.get('game_id')
        timestamp = message.get('timestamp')
        data = message.get('data', {})
        
        logger.info(f"Processing game event: {event_type} for game {game_id}")
        
        # In a real implementation, we would handle different types of game events
        # For now, let's just log the event
        logger.info(f"Game event data: {data}")

class FantasyTransactionsConsumer(threading.Thread):
    """Consumer for fantasy transactions topic"""
    
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            'fantasy-transactions',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def stop(self):
        """Stop the consumer thread"""
        self.stop_event.set()
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting fantasy transactions consumer")
        
        while not self.stop_event.is_set():
            for message in self.consumer:
                if self.stop_event.is_set():
                    break
                
                try:
                    # Process the message
                    self.process_transaction(message.value)
                except Exception as e:
                    logger.error(f"Error processing fantasy transaction: {e}")
            
            time.sleep(0.1)  # Small sleep to prevent CPU hogging
        
        # Close the consumer
        self.consumer.close()
        logger.info("Fantasy transactions consumer stopped")
    
    def process_transaction(self, message):
        """Process fantasy transaction messages"""
        source = message.get('source')
        msg_type = message.get('type')
        timestamp = message.get('timestamp')
        data = message.get('data', {})
        
        logger.info(f"Processing fantasy transaction from {source}: {msg_type}")
        
        # In a real implementation, this would handle different transaction types
        # For now, let's just log the transaction
        logger.info(f"Fantasy transaction data: {data}")

def main():
    """Main function to start all consumers"""
    logger.info("Starting Kafka consumers for fantasy football data")
    
    # Start the consumers
    consumers = [
        PlayerStatsConsumer(),
        PlayerInjuriesConsumer(),
        GameEventsConsumer(),
        FantasyTransactionsConsumer()
    ]
    
    for consumer in consumers:
        consumer.start()
    
    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Stop all consumers on keyboard interrupt
        logger.info("Stopping consumers...")
        for consumer in consumers:
            consumer.stop()
        
        # Wait for all consumers to stop
        for consumer in consumers:
            consumer.join()

if __name__ == "__main__":
    main()