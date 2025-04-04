from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, MapType
import os

def main():
    """
    Spark job to process player data from multiple sources and consolidate into a unified format
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Fantasy Football - Player Data Processing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    # Define schemas for different data sources
    
    # ESPN Player Schema
    espn_player_schema = StructType([
        StructField("id", StringType(), True),
        StructField("fullName", StringType(), True),
        StructField("proTeamId", IntegerType(), True),
        StructField("position", StringType(), True),
        # Additional fields would be here in a real implementation
    ])
    
    # NFL Player Schema
    nfl_player_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("position", StringType(), True),
        StructField("teamId", StringType(), True),
        # Additional fields would be here in a real implementation
    ])
    
    # Sleeper Player Schema
    sleeper_player_schema = StructType([
        StructField("player_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("position", StringType(), True),
        StructField("team", StringType(), True),
        # Additional fields would be here in a real implementation
    ])
    
    # Read player data from MinIO
    # In a real implementation, we would use more robust credentials management
    
    # Read ESPN player data
    espn_players_df = spark.read.json(
        "s3a://raw-espn-data/players/",
        schema=espn_player_schema
    )
    
    # Read NFL player data
    nfl_players_df = spark.read.json(
        "s3a://raw-nfl-data/players/",
        schema=nfl_player_schema
    )
    
    # Read Sleeper player data
    sleeper_players_df = spark.read.json(
        "s3a://raw-sleeper-data/players/",
        schema=sleeper_player_schema
    )
    
    # Transform ESPN data to common format
    espn_players_transformed = espn_players_df.select(
        col("id").alias("source_id"),
        col("fullName").alias("name"),
        col("position"),
        col("proTeamId").alias("team_id"),
        lit("espn").alias("source")
    )
    
    # Transform NFL data to common format
    nfl_players_transformed = nfl_players_df.select(
        col("id").alias("source_id"),
        col("name"),
        col("position"),
        col("teamId").alias("team_id"),
        lit("nfl").alias("source")
    )
    
    # Transform Sleeper data to common format
    sleeper_players_transformed = sleeper_players_df.select(
        col("player_id").alias("source_id"),
        col("full_name").alias("name"),
        col("position"),
        col("team").alias("team_id"),
        lit("sleeper").alias("source")
    )
    
    # Union all data sources
    all_players = espn_players_transformed.union(nfl_players_transformed).union(sleeper_players_transformed)
    
    # Add processing timestamp
    all_players = all_players.withColumn("processed_at", current_timestamp())
    
    # Write consolidated data to the processed zone
    all_players.write \
        .mode("overwrite") \
        .parquet("s3a://processed-player-data/unified_players/")
    
    # Create a view for players to use in analysis
    all_players.createOrReplaceTempView("players")
    
    # Example analysis: Count players by position
    position_counts = spark.sql("""
        SELECT position, COUNT(*) as player_count
        FROM players
        GROUP BY position
        ORDER BY player_count DESC
    """)
    
    # Write analysis results
    position_counts.write \
        .mode("overwrite") \
        .parquet("s3a://analytics-results/player_position_counts/")
    
    # Example analysis: Find players with multiple sources
    duplicate_players = spark.sql("""
        SELECT name, position, COUNT(DISTINCT source) as source_count, 
               COLLECT_LIST(source) as sources
        FROM players
        GROUP BY name, position
        HAVING COUNT(DISTINCT source) > 1
        ORDER BY source_count DESC
    """)
    
    # Write duplicate analysis results
    duplicate_players.write \
        .mode("overwrite") \
        .parquet("s3a://analytics-results/player_multiple_sources/")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()