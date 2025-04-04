from minio import Minio
from minio.error import S3Error
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_minio_buckets():
    """
    Set up MinIO buckets for data lake storage
    """
    # Initialize MinIO client
    minio_client = Minio(
        "localhost:9000",
        access_key="fantasy",
        secret_key="fantasy123",
        secure=False
    )
    
    # Define bucket names
    buckets = [
        "raw-nfl-data",
        "raw-espn-data",
        "raw-yahoo-data",
        "raw-sleeper-data",
        "raw-pfr-data",
        "processed-player-data",
        "processed-game-data",
        "processed-fantasy-data",
        "analytics-results"
    ]
    
    # Create buckets if they don't exist
    for bucket in buckets:
        try:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)
                logger.info(f"Bucket '{bucket}' created successfully")
            else:
                logger.info(f"Bucket '{bucket}' already exists")
        except S3Error as e:
            logger.error(f"Error creating bucket '{bucket}': {e}")

if __name__ == "__main__":
    setup_minio_buckets()