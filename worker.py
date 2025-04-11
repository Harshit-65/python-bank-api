import logging
import time
import redis # Import redis exceptions
from arq import run_worker
from arq.connections import RedisSettings as ArqRedisSettings # Import ARQ's RedisSettings

# Import WorkerSettings from your tasks file
from app.queue.tasks import WorkerSettings
# Import your App's RedisSettings to get the loaded values
from app.config import RedisSettings as AppRedisSettings 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('arq.worker').setLevel(logging.WARNING)
# Create ARQ RedisSettings object from loaded config values
arq_redis_settings = ArqRedisSettings(
    host=AppRedisSettings.redis_url.split("://")[1].split(":")[0],
    port=int(AppRedisSettings.redis_url.split(":")[2].split("/")[0]),
    password=AppRedisSettings.redis_password, 
    # Add other necessary settings from ARQ's RedisSettings if needed (e.g., database=0)
    # Consider adding connection_timeout if needed
    # conn_timeout=10 # Example: 10 second timeout
)

if __name__ == "__main__":
    while True: # Loop to automatically restart worker on crash
        try:
            # Using print before logging in case logging itself fails early
            print("Starting ARQ worker...") 
            logging.info("ARQ worker starting...")
            # Run the worker (this blocks until the worker stops or crashes)
            run_worker(WorkerSettings, redis_settings=arq_redis_settings)
            # If run_worker exits cleanly (e.g., Ctrl+C), break the loop
            logging.info("ARQ worker stopped cleanly.")
            break
        except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError) as e:
            logging.error(f"ARQ Worker crashed due to Redis connection error: {e}. Restarting in 15 seconds...")
            time.sleep(15) # Wait before restarting
        except Exception as e:
            # Catch other unexpected errors
            logging.error(f"ARQ Worker crashed unexpectedly: {e}. Restarting in 15 seconds...")
            time.sleep(15)
        # If loop continues, it means run_worker crashed and we try again 