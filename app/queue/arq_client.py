import logging
import os
from typing import Any, Optional
from arq import create_pool
from arq.connections import RedisSettings

from ..config import RedisSettings as AppRedisSettings

logger = logging.getLogger(__name__)

arq_redis_settings = RedisSettings(
    host=AppRedisSettings.redis_url.split("://")[1].split(":")[0],
    port=int(AppRedisSettings.redis_url.split(":")[2].split("/")[0]),
    # Add password handling if your Redis requires it
    password=os.getenv("REDIS_PASSWORD"),
)

async def get_arq_redis():
    """FastAPI dependency to get ARQ Redis pool."""
    # Note: For simplicity, creating pool here.
    # In complex apps, manage pool lifecycle with app startup/shutdown events.
    redis = await create_pool(arq_redis_settings)
    return redis

async def enqueue_job(ctx: Any, function_name: str, *args: Any, **kwargs: Any):
    """Enqueues a job using the ARQ client from the context."""
    try:
        redis = ctx.get("arq_redis")
        if not redis:
            logger.error("ARQ Redis client not found in context.")
            # In a real app, might fallback to creating a pool or raise an error
            # For now, try creating a temporary pool (less efficient)
            redis = await create_pool(arq_redis_settings)
            if not redis:
                raise ConnectionError("Failed to connect to ARQ Redis")
            await redis.enqueue_job(function_name, *args, **kwargs)
            await redis.close()
        else:
            await redis.enqueue_job(function_name, *args, **kwargs)
        logger.info(f"Enqueued job '{function_name}' with args: {args}, kwargs: {kwargs}")
    except Exception as e:
        logger.error(f"Error enqueuing job '{function_name}': {e}")
        # Decide how to handle enqueue errors (e.g., raise exception, retry)
        raise 