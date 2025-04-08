from supabase import create_client, Client
from ..config import SUPABASE_URL, SUPABASE_KEY

supabase: Client | None = None

def get_supabase_client() -> Client:
    """Initializes and returns the Supabase client."""
    print("Executing in: supabase_client.py - get_supabase_client")
    global supabase
    if supabase is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase URL and Key must be set in environment variables.")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase 