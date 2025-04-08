from typing import Annotated, Dict, Any
import httpx
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer

from ..config import KEYFOLIO_URL
from ..db.supabase_client import get_supabase_client
from .models import KeyfolioResponse
from supabase import Client

# Although we use Bearer tokens, OAuth2PasswordBearer is a convenient way 
# to extract the token from the Authorization header.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False) # auto_error=False to handle missing header manually

class APIKeyNotFound(HTTPException):
    def __init__(self):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "INVALID_TOKEN", "message": "Invalid API key", "details": "API key not found in database"},
            headers={"WWW-Authenticate": "Bearer"},
        )

class KeyfolioVerificationError(HTTPException):
    def __init__(self):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "code": "INTERNAL_SERVER_ERROR",
                "message": "Failed to verify API key",
                "details": "The authentication service is temporarily unavailable",
            },
        )

class InvalidTokenError(HTTPException):
    def __init__(self):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "INVALID_TOKEN", "message": "Invalid API key", "details": "The provided API key is not valid"},
            headers={"WWW-Authenticate": "Bearer"},
        )

class RateLimitExceededError(HTTPException):
    def __init__(self, reset_after_ms: int):
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "code": "RATE_LIMIT_EXCEEDED",
                "message": "Rate limit exceeded",
                "details": f"Remaining: 0, Reset after: {reset_after_ms}ms",
            },
            headers={"Retry-After": str(reset_after_ms // 1000)}, # Retry-After is in seconds
        )

async def _get_user_from_supabase(api_key: str, supabase: Client) -> Dict[str, Any]:
    """Internal helper: Fetches user details from Supabase using the API key hash."""
    try:
        # Use sync client for Supabase v1 DB ops
        response = supabase.table("api_keys").select("id, user_id").eq("key_hash", api_key).execute()
        if not response.data:
            raise APIKeyNotFound()
        # Assuming one key matches
        return response.data[0]
    except APIKeyNotFound:
        raise # Re-raise specific exception
    except Exception as e:
        print(f"Error querying Supabase for user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "code": "INTERNAL_SERVER_ERROR",
                "message": "Failed to verify API key",
                "details": "Error fetching user from database",
            },
        )

async def _perform_key_verification(api_key: str, supabase: Client) -> Dict[str, Any]:
    """Performs Keyfolio check and fetches user data from Supabase.
    Raises specific HTTPExceptions on failure.
    Returns user data dictionary (user_id, api_key_id) on success.
    """
    # 1. Verify with Keyfolio
    async with httpx.AsyncClient() as client:
        try:
            headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
            response = await client.post(f"{KEYFOLIO_URL}/api/verify", headers=headers)
            response.raise_for_status() # Raise exception for 4xx/5xx responses
        except httpx.RequestError as exc:
            print(f"HTTP Request Error verifying key with Keyfolio: {exc}")
            raise KeyfolioVerificationError()
        except httpx.HTTPStatusError as exc:
            # Handle specific Keyfolio error responses if needed, otherwise treat as generic failure
            print(f"Keyfolio service error: {exc.response.status_code} - {exc.response.text}")
            raise KeyfolioVerificationError()

    try:
        keyfolio_data = response.json()
        keyfolio_resp = KeyfolioResponse(**keyfolio_data)
    except Exception as e: # Includes JSONDecodeError and Pydantic ValidationError
        print(f"Error decoding Keyfolio response: {e}")
        raise KeyfolioVerificationError()

    if not keyfolio_resp.valid:
        raise InvalidTokenError()

    if keyfolio_resp.remaining <= 0 and keyfolio_resp.reset_after_ms is not None:
        raise RateLimitExceededError(keyfolio_resp.reset_after_ms)

    # 2. Get user from Supabase (using the validated API key)
    user_data = await _get_user_from_supabase(api_key, supabase)
    
    # Return essential data
    return {"user_id": user_data["user_id"], "api_key_id": user_data["id"]}


async def verify_api_key(request: Request,
                       token: Annotated[str | None, Depends(oauth2_scheme)],
                       supabase: Annotated[Client, Depends(get_supabase_client)]) -> Dict[str, Any]:
    """FastAPI dependency to verify API key via Keyfolio and fetch user data.
    
    This function now primarily acts as a wrapper for the core logic
    and handles storing data in request.state.
    """
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "MISSING_AUTH_HEADER", "message": "Missing Authorization header", "details": "Include 'Authorization: Bearer YOUR_API_KEY'"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Call the core verification logic
    user_data = await _perform_key_verification(token, supabase)

    # Store auth data in request state for logging middleware
    request.state.user_id = user_data.get("user_id")
    request.state.api_key_id = user_data.get("api_key_id")

    # Return user data dictionary
    return user_data

# Type alias for dependency injection in route handlers
# This remains useful for endpoints where direct injection is okay
AuthData = Annotated[Dict[str, Any], Depends(verify_api_key)] 