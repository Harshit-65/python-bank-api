import logging
import os
from typing import Any, Dict
from arq import create_pool, ArqRedis
from arq.connections import RedisSettings
from ..config import RedisSettings as AppRedisSettings

logger = logging.getLogger(__name__)

# Shared Redis settings
arq_redis_settings = RedisSettings(
    host=AppRedisSettings.redis_url.split("://")[1].split(":")[0],
    port=int(AppRedisSettings.redis_url.split(":")[2].split("/")[0]),
    password=os.getenv("REDIS_PASSWORD"),
)

# Global pool reference
arq_pool: ArqRedis | None = None

async def get_arq_redis() -> ArqRedis:
    """FastAPI dependency to get ARQ Redis pool."""
    print("Executing in: arq_client.py - get_arq_redis")
    if arq_pool is None:
        raise RuntimeError("ARQ Redis pool accessed before initialization")
    return arq_pool

async def init_arq_pool():
    """Initialize the global ARQ pool (called from lifespan)"""
    global arq_pool
    arq_pool = await create_pool(arq_redis_settings)
    logger.info("ARQ Redis pool initialized")

async def close_arq_pool():
    """Close the global ARQ pool (called from lifespan)"""
    global arq_pool
    if arq_pool:
        await arq_pool.close()
        arq_pool = None
        logger.info("ARQ Redis pool closed")

async def enqueue_job(ctx: Any, function_name: str, job_params: Dict[str, Any]):
    """Enqueues a job using the ARQ client from the context."""
    try:
        redis = await get_arq_redis()
        await redis.enqueue_job(function_name, **job_params)
        logger.info(f"Enqueued job '{function_name}'")
    except Exception as e:
        logger.error(f"Error enqueuing job '{function_name}': {e}")
        raise