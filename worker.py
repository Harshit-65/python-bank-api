import logging
from arq import run_worker
from arq.connections import RedisSettings as ArqRedisSettings # Import ARQ's RedisSettings

# Import WorkerSettings from your tasks file
from app.queue.tasks import WorkerSettings
# Import your App's RedisSettings to get the loaded values
from app.config import RedisSettings as AppRedisSettings 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create ARQ RedisSettings object from loaded config values
arq_redis_settings = ArqRedisSettings(
    host=AppRedisSettings.redis_url.split("://")[1].split(":")[0],
    port=int(AppRedisSettings.redis_url.split(":")[2].split("/")[0]),
    password=AppRedisSettings.redis_password, 
    # Add other necessary settings from ARQ's RedisSettings if needed (e.g., database=0)
)

if __name__ == "__main__":
    # Call run_worker directly, passing the settings.
    # It will handle the async event loop internally.
    run_worker(WorkerSettings, redis_settings=arq_redis_settings) 