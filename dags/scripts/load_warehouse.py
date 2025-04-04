#!/usr/bin/env python
import os
import json
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pyarrow.parquet as pq
import io
import boto3
import logging
from datetime import datetime
from psycopg2 import cursor

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

# MinIO connection parameters
MINIO_PARAMS = {
    'endpoint_url': os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000'),
    'aws_access_key_id': os.environ.get('MINIO_ACCESS_KEY', 'fantasy'),
    'aws_secret_access_key': os.environ.get('MINIO_SECRET_KEY', 'fantasy123')
}

def get_s3_client():
    """Get a boto3 S3 client configured for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_PARAMS['endpoint_url'],
        aws_access_key_id=MINIO_PARAMS['aws_access_key_id'],
        aws_secret_access_key=MINIO_PARAMS['aws_secret_access_key']
    )

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    return psycopg2.connect(**DB_PARAMS)

def read_parquet_from_minio(bucket, prefix):
    """Read Parquet files from MinIO and return as a pandas DataFrame"""
    s3_client = get_s3_client()
    
    # List all objects with the given prefix
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    # Initialize an empty list to hold DataFrames
    dfs = []
    
    # Process each object
    for obj in objects.get('Contents', []):
        # Skip directories
        if obj['Key'].endswith('/'):
            continue
            
        # Only process .parquet files
        if not obj['Key'].endswith('.parquet'):
            continue
            
        logger.info(f"Reading {obj['Key']} from {bucket}")
        
        # Get the object
        response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
        
        # Read the Parquet data
        parquet_buffer = io.BytesIO(response['Body'].read())
        table = pq.read_table(parquet_buffer)
        df = table.to_pandas()
        
        dfs.append(df)
    
    # Combine all DataFrames
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        logger.warning(f"No Parquet files found in {bucket}/{prefix}")
        return pd.DataFrame()

def load_players():
    """Load player data into the data warehouse"""
    logger.info("Loading player data into the data warehouse")
    
    # Read processed player data
    df = read_parquet_from_minio('processed-player-data', 'unified_players/')
    
    if df.empty:
        logger.warning("No player data found")
        return
    
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Prepare data for insertion
        player_data = []
        for _, row in df.iterrows():
            player_data.append((
                row.get('name'),
                row.get('position'),
                row.get('team_id'),
                None,  # age (not available in our dataset)
                None,  # experience
                None,  # birth_date
                None,  # college
                None,  # height
                None,  # weight
                None,  # draft_date
                None,  # draft_round
                None   # draft_position
            ))
        
        # Insert data into the players table
        query = """
        INSERT INTO players (
            name, position, team, age, experience, birth_date, college,
            height, weight, draft_date, draft_round, draft_position
        )
        VALUES %s
        ON CONFLICT (player_id) DO UPDATE SET
            name = EXCLUDED.name,
            position = EXCLUDED.position,
            team = EXCLUDED.team,
            updated_at = CURRENT_TIMESTAMP
        RETURNING player_id
        """
        
        execute_values(cursor, query, player_data)
        player_ids = [row[0] for row in cursor.fetchall()]
        
        conn.commit()
        logger.info(f"Loaded {len(player_ids)} players into the warehouse")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading player data: {e}")
    finally:
        cursor.close()
        conn.close()

def load_games():
    """Load game data into the data warehouse"""
    logger.info("Loading game data into the data warehouse")
    
    # Read processed game data
    df = read_parquet_from_minio('processed-game-data', 'unified_games/')
    
    if df.empty:
        logger.warning("No game data found")
        return
    
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # First, ensure teams exist in the database
        team_ids = set(df['home_team_id'].unique()) | set(df['away_team_id'].unique())
        
        for team_id in team_ids:
            if team_id and pd.notna(team_id):
                cursor.execute("""
                INSERT INTO teams (team_name, abbreviation)
                VALUES (%s, %s)
                ON CONFLICT (team_id) DO NOTHING
                """, (team_id, team_id))
        
        # Prepare game data for insertion
        game_data = []
        for _, row in df.iterrows():
            # Get team IDs
            cursor.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (row.get('home_team_id'),))
            home_team_db_id = cursor.fetchone()
            if home_team_db_id:
                home_team_db_id = home_team_db_id[0]
                
            cursor.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (row.get('away_team_id'),))
            away_team_db_id = cursor.fetchone()
            if away_team_db_id:
                away_team_db_id = away_team_db_id[0]
            
            # Only insert game if we have valid team IDs
            if home_team_db_id and away_team_db_id:
                game_data.append((
                    home_team_db_id,
                    away_team_db_id,
                    row.get('game_date'),
                    row.get('week'),
                    row.get('season'),
                    row.get('stadium'),
                    row.get('weather'),
                    row.get('home_score'),
                    row.get('away_score')
                ))
        
        # Insert data into the player_stats table
        stat_data = []  # Define the "stat_data" variable
        # Add code to populate the "stat_data" list
        
        query = """
        INSERT INTO player_stats (
            player_id, game_id, week, season, stat_type,
            value, position_played, snap_count, fantasy_points
        )
        VALUES %s
        ON CONFLICT (stat_id) DO UPDATE SET
            value = EXCLUDED.value,
            fantasy_points = EXCLUDED.fantasy_points,
            updated_at = CURRENT_TIMESTAMP
        RETURNING stat_id
        """
        
        execute_values(cursor, query, stat_data)  # Pass "stat_data" as an argument
        stat_ids = [row[0] for row in cursor.fetchall()]
        
        conn.commit()
        logger.info(f"Loaded {len(stat_ids)} player stats into the warehouse")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading player stats: {e}")
    finally:
        cursor.close()
        conn.close()

def load_projections():
    """Load player projections into the data warehouse"""
    logger.info("Loading player projections into the data warehouse")
    
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get a list of players
        cursor.execute("SELECT player_id, name, position FROM players")
        players = cursor.fetchall()
        
        if not players:
            logger.warning("No players found in the database")
            return
        
        # Current season and week
        current_season = 2023
        current_week = 10  # For example
        
        # Prepare projection data
        projection_data = []
        
        for player_id, _, position in players:
            if position in ['QB', 'RB', 'WR', 'TE']:
                # Generate projections for the next three weeks
                for week in range(current_week, current_week + 3):
                    # Base projections on player ID and position
                    projected_points = 0
                    
                    if position == 'QB':
                        passing_yards = 250 + (player_id % 100)
                        passing_tds = 1.5 + (player_id % 3) * 0.5
                        ints = 0.5 + (player_id % 2) * 0.5
                        rush_yards = 10 + (player_id % 20)
                        
                        projected_points = (
                            passing_yards * 0.04 +  # 1 point per 25 yards
                            passing_tds * 4 +       # 4 points per TD
                            ints * -2 +             # -2 points per INT
                            rush_yards * 0.1        # 1 point per 10 yards
                        )
                        
                        # Passing yards projection
                        projection_data.append((
                            player_id,
                            week,
                            current_season,
                            'espn',  # source
                            projected_points,
                            'passing_yards',
                            passing_yards
                        ))
                        
                        # Passing TDs projection
                        projection_data.append((
                            player_id,
                            week,
                            current_season,
                            'espn',  # source
                            projected_points,
                            'passing_tds',
                            passing_tds
                        ))
                        
                    elif position == 'RB':
                        rush_yards = 60 + (player_id % 40)
                        rush_tds = 0.5 + (player_id % 2) * 0.5
                        receptions = 2 + (player_id % 4)
                        rec_yards = 15 + (player_id % 25)
                        
                        projected_points = (
                            rush_yards * 0.1 +     # 1 point per 10 yards
                            rush_tds * 6 +         # 6 points per TD
                            receptions * 0.5 +     # 0.5 per reception (PPR)
                            rec_yards * 0.1        # 1 point per 10 yards
                        )
                        
                        # Rushing yards projection
                        projection_data.append((
                            player_id,
                            week,
                            current_season,
                            'espn',  # source
                            projected_points,
                            'rushing_yards',
                            rush_yards
                        ))
                        
                    elif position == 'WR' or position == 'TE':
                        receptions = 4 + (player_id % 7)
                        rec_yards = 50 + (player_id % 70)
                        rec_tds = 0.3 + (player_id % 3) * 0.3
                        
                        projected_points = (
                            receptions * 0.5 +     # 0.5 per reception (PPR)
                            rec_yards * 0.1 +      # 1 point per 10 yards
                            rec_tds * 6            # 6 points per TD
                        )
                        
                        # Receiving yards projection
                        projection_data.append((
                            player_id,
                            week,
                            current_season,
                            'espn',  # source
                            projected_points,
                            'receiving_yards',
                            rec_yards
                        ))
                    
                    # Also add Yahoo and Sleeper projections with slight variations
                    for source in ['yahoo', 'sleeper']:
                        variance = 0.9 + ((player_id + ord(source[0])) % 3) * 0.1  # 0.9-1.1
                        
                        projection_data.append((
                            player_id,
                            week,
                            current_season,
                            source,  # source
                            projected_points * variance,
                            'total',
                            None  # No specific stat value
                        ))
        
        # Insert data into the projections table
        query = """
        INSERT INTO projections (
            player_id, week, season, source, 
            projected_points, stat_category, projected_value
        )
        VALUES %s
        ON CONFLICT (projection_id) DO UPDATE SET
            projected_points = EXCLUDED.projected_points,
            updated_at = CURRENT_TIMESTAMP
        RETURNING projection_id
        """
        
        execute_values(cursor, query, projection_data)
        projection_ids = [row[0] for row in cursor.fetchall()]
        
        conn.commit()
        logger.info(f"Loaded {len(projection_ids)} projections into the warehouse")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading projections: {e}")
    finally:
        cursor.close()
        conn.close()

def load_injuries():
    """Load player injury data into the data warehouse"""
    logger.info("Loading player injuries into the data warehouse")
    
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get a list of players
        cursor.execute("SELECT player_id, name FROM players LIMIT 20")
        players = cursor.fetchall()
        
        if not players:
            logger.warning("No players found in the database")
            return
        
        # Sample injury types and body parts
        injury_types = ["Sprain", "Strain", "Concussion", "Fracture", "Contusion"]
        body_parts = ["Ankle", "Knee", "Shoulder", "Hamstring", "Head", "Foot", "Hand", "Back"]
        practice_statuses = ["Did Not Practice", "Limited", "Full Participation"]
        game_statuses = ["Out", "Doubtful", "Questionable", "Probable"]
        
        # Prepare injury data - simulate injuries for some players
        injury_data = []
        today = datetime.now().date()
        
        for player_id, _ in players:
            # Only create injuries for some players (about 25%)
            if player_id % 4 == 0:
                injury_type = injury_types[player_id % len(injury_types)]
                body_part = body_parts[player_id % len(body_parts)]
                practice_status = practice_statuses[player_id % len(practice_statuses)]
                game_status = game_statuses[player_id % len(game_statuses)]
                
                injury_data.append((
                    player_id,
                    today,  # report_date
                    injury_type,
                    body_part,
                    practice_status,
                    game_status,
                    f"Player reported {injury_type.lower()} in {body_part.lower()} during practice.",  # notes
                    None  # return_date (unknown yet)
                ))
        
        # Insert data into the injuries table
        query = """
        INSERT INTO injuries (
            player_id, report_date, injury_type, body_part,
            practice_status, game_status, notes, return_date
        )
        VALUES %s
        ON CONFLICT (injury_id) DO UPDATE SET
            practice_status = EXCLUDED.practice_status,
            game_status = EXCLUDED.game_status,
            notes = EXCLUDED.notes,
            updated_at = CURRENT_TIMESTAMP
        RETURNING injury_id
        """
        
        execute_values(cursor, query, injury_data)
        injury_ids = [row[0] for row in cursor.fetchall()]
        
        conn.commit()
        logger.info(f"Loaded {len(injury_ids)} injuries into the warehouse")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading injuries: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to load all data into the warehouse"""
    logger.info("Starting data warehouse loading process")
    
    # Load data in a specific order to maintain referential integrity
    load_players()
    load_games()
    load_player_stats()
    load_projections()
    load_injuries()
    
    logger.info("Data warehouse loading process completed")

if __name__ == "__main__":
    main()
        
    # First check if a similar game exists
    check_query = """
    SELECT game_id 
    FROM games 
    WHERE home_team_id = %s AND away_team_id = %s AND game_date = %s AND season = %s AND week = %s
    """

    # Insert data into the games table
    insert_query = """
    INSERT INTO games (
        home_team_id, away_team_id, game_date, week, season, 
        stadium, weather_conditions, home_score, away_score
    )
    VALUES %s
    RETURNING game_id
    """

    # Update existing games
    conn = get_db_connection()  # Connect to the database
    update_query = """
    UPDATE games
    SET home_score = %s, away_score = %s, updated_at = CURRENT_TIMESTAMP
    WHERE game_id = %s
    """

    game_ids = []
    game_data = []  # Define the variable "game_data" as an empty list

    # Rest of the code
    # ...

    conn.commit()
    logger.info(f"Loaded {len(game_ids)} games into the warehouse")

def load_player_stats():
    """Load player statistics into the data warehouse"""
    logger.info("Loading player statistics into the data warehouse")
    
    # This would be a more complex implementation in a real system
    # For demonstration purposes, we'll create sample data
    
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Code inside the try block
        pass
    except Exception as e:
        # Handle the exception
        logger.error(f"Error loading player statistics: {e}")
    
        # Get a list of players
        cursor.execute("SELECT player_id, name FROM players LIMIT 10")
        players = cursor.fetchall()
        
        # Get a list of games
        cursor.execute("SELECT game_id, week, season FROM games LIMIT 10")
        games = cursor.fetchall()
        
        if not players or not games:
            logger.warning("No players or games found in the database")
            return
        
        # Prepare sample stat data
        stat_data = []
        for player_id, _ in players:
            for game_id, week, season in games:
                # Create some realistic sample stats
                stat_data.append((
                    player_id,
                    game_id,
                    week,
                    season,
                    "passing_yards",  # stat_type
                    float(150 + (player_id * 10) % 100),  # value
                    "QB" if player_id % 5 == 0 else "WR",  # position_played
                    (30 + player_id % 40),  # snap_count
                    float(10 + (player_id * 5) % 20)  # fantasy_points
                ))
                
                # Add another stat for the same player/game
                stat_data.append((
                    player_id,
                    game_id,
                    week,
                    season,
                    "touchdowns",  # stat_type
                    float(1 + (player_id * 3) % 4),  # value
                    "QB" if player_id % 5 == 0 else "WR",  # position_played
                    (30 + player_id % 40),  # snap_count
                    float(6 + (player_id * 3) % 12)  # fantasy_points
                ))