import os
from dotenv import load_dotenv
from redis.asyncio import Redis, from_url # Import for Redis settings

load_dotenv()  # Load variables from .env file

KEYFOLIO_URL = os.getenv("KEYFOLIO_URL", "https://keyfolio-febpthnnhq-ue.a.run.app")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") 
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Google API Keys - support multiple keys for load balancing
GOOGLE_API_KEYS = []
for i in range(1, 16):  # Support up to 15 keys
    key = os.getenv(f"GOOGLE_API_KEY{i}" if i > 1 else "GOOGLE_API_KEY")
    if key:
        GOOGLE_API_KEYS.append(key)

# Default to the first key for backward compatibility
GOOGLE_API_KEY = GOOGLE_API_KEYS[0] if GOOGLE_API_KEYS else None

# ARQ Redis Settings
class RedisSettings:
    redis_url = REDIS_URL
    redis_password = REDIS_PASSWORD

    # Function to create a Redis connection pool for ARQ
    @staticmethod
    async def create_pool() -> Redis:
        return await from_url(
            RedisSettings.redis_url, 
            encoding="utf8", 
            decode_responses=True,
            password=RedisSettings.redis_password
        )

# Validate essential config
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL and Key must be set in environment variables.")

if not GOOGLE_API_KEY:
    print("Warning: GOOGLE_API_KEY not set. AI processing will fail.") 