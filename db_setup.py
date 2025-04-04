import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    """Create the fantasy football database if it doesn't exist"""
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="fantasy",
        password="fantasy123"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'fantasy_football'")
    exists = cursor.fetchone()
    
    if not exists:
        cursor.execute("CREATE DATABASE fantasy_football")
        print("Database created successfully")
    else:
        print("Database already exists")
    
    cursor.close()
    conn.close()

def create_tables():
    """Create the tables for the fantasy football data warehouse"""
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="fantasy",
        password="fantasy123",
        database="fantasy_football"
    )
    cursor = conn.cursor()
    
    # Create tables based on our data model
    
    # Players dimension table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        position VARCHAR(10),
        team VARCHAR(50),
        age INT,
        experience INT,
        birth_date DATE,
        college VARCHAR(100),
        height FLOAT,
        weight FLOAT,
        draft_date DATE,
        draft_round INT,
        draft_position INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Teams dimension table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        team_id SERIAL PRIMARY KEY,
        team_name VARCHAR(100) NOT NULL,
        abbreviation VARCHAR(10),
        conference VARCHAR(10),
        division VARCHAR(20),
        head_coach VARCHAR(100),
        offensive_coordinator VARCHAR(100),
        defensive_coordinator VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Games dimension table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_id SERIAL PRIMARY KEY,
        home_team_id INT REFERENCES teams(team_id),
        away_team_id INT REFERENCES teams(team_id),
        game_date DATE,
        week INT,
        season INT,
        stadium VARCHAR(100),
        weather_conditions VARCHAR(100),
        home_score INT,
        away_score INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Player stats fact table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS player_stats (
        stat_id SERIAL PRIMARY KEY,
        player_id INT REFERENCES players(player_id),
        game_id INT REFERENCES games(game_id),
        week INT,
        season INT,
        stat_type VARCHAR(50),
        value FLOAT,
        position_played VARCHAR(10),
        snap_count INT,
        fantasy_points FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Fantasy leagues dimension table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fantasy_leagues (
        league_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        platform VARCHAR(50),
        teams_count INT,
        scoring_format VARCHAR(50),
        roster_settings TEXT,
        draft_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Fantasy teams dimension table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fantasy_teams (
        fantasy_team_id SERIAL PRIMARY KEY,
        league_id INT REFERENCES fantasy_leagues(league_id),
        team_name VARCHAR(100),
        owner_name VARCHAR(100),
        current_rank INT,
        points_for FLOAT,
        points_against FLOAT,
        wins INT,
        losses INT,
        ties INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Fantasy rosters fact table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fantasy_rosters (
        roster_id SERIAL PRIMARY KEY,
        fantasy_team_id INT REFERENCES fantasy_teams(fantasy_team_id),
        player_id INT REFERENCES players(player_id),
        acquisition_date DATE,
        acquisition_type VARCHAR(50),
        starting BOOLEAN,
        roster_position VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Projections fact table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS projections (
        projection_id SERIAL PRIMARY KEY,
        player_id INT REFERENCES players(player_id),
        week INT,
        season INT,
        source VARCHAR(50),
        projected_points FLOAT,
        stat_category VARCHAR(50),
        projected_value FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Injuries dimension table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS injuries (
        injury_id SERIAL PRIMARY KEY,
        player_id INT REFERENCES players(player_id),
        report_date DATE,
        injury_type VARCHAR(100),
        body_part VARCHAR(50),
        practice_status VARCHAR(50),
        game_status VARCHAR(50),
        notes TEXT,
        return_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Create indexes for better query performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_stats_player_id ON player_stats(player_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_stats_game_id ON player_stats(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_stats_week_season ON player_stats(week, season)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_projections_player_week_season ON projections(player_id, week, season)")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Tables created successfully")

if __name__ == "__main__":
    create_database()
    create_tables()