# from supabase import create_client, Client
from supabase import create_async_client, AsyncClient
from ..config import SUPABASE_URL, SUPABASE_KEY

supabase: AsyncClient  | None = None

async def get_supabase_client() -> AsyncClient:
    """Initializes and returns the Supabase client."""
    print("Executing in: supabase_client.py - get_supabase_client")
    global supabase
    if supabase is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase URL and Key must be set in environment variables.")
        supabase = await create_async_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase 