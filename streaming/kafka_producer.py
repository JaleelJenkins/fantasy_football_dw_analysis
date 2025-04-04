#!/usr/bin/env python
import json
import logging
import time
import threading
import schedule
from datetime import datetime
import os
import sys

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.api_connectors import ESPNConnector, NFLConnector, SleeperConnector, PFRScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka connection parameters
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class DataCollector:
    """
    Class to manage data collection from various sources
    """
    def __init__(self):
        # Initialize API connectors
        self.espn = ESPNConnector(
            league_id=os.environ.get('ESPN_LEAGUE_ID', '12345'),
            year=int(os.environ.get('CURRENT_SEASON', '2023')),
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
        )
        
        self.nfl = NFLConnector(
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
        )
        
        self.sleeper = SleeperConnector(
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
        )
        
        self.pfr = PFRScraper(
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
        )
        
        # Set up schedules
        self._setup_schedules()
    
    def _setup_schedules(self):
        """Set up collection schedules"""
        # Daily collection jobs
        schedule.every().day.at("04:00").do(self.collect_espn_players)
        schedule.every().day.at("05:00").do(self.collect_nfl_stats)
        schedule.every().day.at("06:00").do(self.collect_sleeper_players)
        schedule.every().day.at("07:00").do(self.collect_pfr_stats)
        
        # Weekly collection jobs
        schedule.every().monday.at("08:00").do(self.collect_weekly_stats)
        
        # Hourly collection jobs
        schedule.every(1).hours.do(self.collect_sleeper_injuries)
        
        # Live game collection (during game times)
        # This would be more complex in a real system, based on game schedules
        schedule.every(5).minutes.do(self.collect_live_updates)
    
    def run_scheduler(self):
        """Run the scheduler"""
        logger.info("Starting scheduler")
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def collect_all(self):
        """Collect all data (for initial loading)"""
        logger.info("Collecting all data")
        
        # Collect player data
        self.collect_espn_players()
        self.collect_sleeper_players()
        
        # Collect game data
        self.collect_nfl_stats()
        
        # Collect statistics
        self.collect_pfr_stats()
        
        # Collect injuries
        self.collect_sleeper_injuries()
        
        logger.info("All data collected successfully")
    
    def collect_espn_players(self):
        """Collect player data from ESPN"""
        logger.info("Collecting ESPN player data")
        
        # In a real implementation, we would paginate through all players
        # For demo purposes, we'll just get a few pages
        for offset in range(0, 150, 50):
            players = self.espn.get_players(limit=50, offset=offset)
            logger.info(f"Collected {len(players)} ESPN players (offset {offset})")
            
            # Sleep to prevent rate limiting
            time.sleep(1)
    
    def collect_sleeper_players(self):
        """Collect player data from Sleeper"""
        logger.info("Collecting Sleeper player data")
        
        players = self.sleeper.get_players()
        logger.info(f"Collected Sleeper players data")
        
        # Get league data for a sample league
        league_id = os.environ.get('SLEEPER_LEAGUE_ID', '12345')
        league = self.sleeper.get_league(league_id)
        logger.info(f"Collected Sleeper league data for {league_id}")
    
    def collect_sleeper_injuries(self):
        """Collect injury data from Sleeper"""
        logger.info("Collecting Sleeper injury data")
        
        injuries = self.sleeper.get_injuries()
        logger.info("Collected Sleeper injury data")
    
    def collect_pfr_stats(self):
        """Collect stats from Pro Football Reference"""
        logger.info("Collecting PFR stats")
        
        current_season = int(os.environ.get('CURRENT_SEASON', '2023'))
        
        # Collect stats by type
        for stat_type in ['passing', 'rushing', 'receiving']:
            stats = self.pfr.get_player_stats(season=current_season, stat_type=stat_type)
            logger.info(f"Collected PFR {stat_type} stats for {current_season}")
            
            # Sleep to prevent rate limiting
            time.sleep(5)  # Longer sleep for web scraping
        
        # Collect game logs for some sample players
        sample_player_ids = [
            'MahoP00',  # Patrick Mahomes
            'KellT00',  # Travis Kelce
            'JeffJ00',  # Justin Jefferson
            'BrowA04',  # A.J. Brown
        ]
        
        for player_id in sample_player_ids:
            game_log = self.pfr.get_player_gamelog(player_id, current_season)
            logger.info(f"Collected PFR game log for {player_id} in {current_season}")
            
            # Sleep to prevent rate limiting
            time.sleep(5)  # Longer sleep for web scraping
    
    def collect_weekly_stats(self):
        """Collect weekly stats after games"""
        logger.info("Collecting weekly stats")
        
        current_season = int(os.environ.get('CURRENT_SEASON', '2023'))
        current_week = int(os.environ.get('CURRENT_WEEK', '1'))
        
        # Collect NFL stats for the current week
        weekly_stats = self.nfl.get_player_stats(season=current_season, week=current_week)
        logger.info(f"Collected NFL week {current_week} stats for {current_season}")
        
        # Collect PFR stats for key players
        sample_player_ids = [
            'MahoP00',  # Patrick Mahomes
            'KellT00',  # Travis Kelce
            'JeffJ00',  # Justin Jefferson
            'BrowA04',  # A.J. Brown
        ]
        
        for player_id in sample_player_ids:
            game_log = self.pfr.get_player_gamelog(player_id, current_season)
            logger.info(f"Collected PFR game log for {player_id} in {current_season}")
            
            # Sleep to prevent rate limiting
            time.sleep(5)
    
    def collect_live_updates(self):
        """Collect live updates during games"""
        logger.info("Collecting live updates")
        
        # In a real implementation, this would check if games are currently in progress
        # and collect real-time data from APIs that provide live updates.
        
        # For demonstration purposes, we'll just log a message
        logger.info("Live update collection would happen here")

def main():
    """Main function to start the data collector"""
    logger.info("Starting Fantasy Football data collector")
    
    collector = DataCollector()
    
    # Collect all data initially
    collector.collect_all()
    
    # Run the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=collector.run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    
    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping data collector...")

if __name__ == "__main__":
    main()
    
    def collect_nfl_stats(self):
        """Collect stats from NFL"""
        logger.info("Collecting NFL stats")
        
        current_season = int(os.environ.get('CURRENT_SEASON', '2023'))
        
        # Get full season stats
        stats = self.nfl.get_player_stats(season=current_season)
        logger.info(f"Collected NFL season stats for {current_season}")
        
        # Get stats for each week
        for week in range(1, 18):  # NFL regular season has 17 weeks
            weekly_stats = self.nfl.get_player_stats(season=current_season, week=week)
            logger.info(f"Collected NFL week {week} stats for {current_season}")
            
            # Sleep to prevent rate limiting
            time.sleep(1)