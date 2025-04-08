import time
import datetime
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from supabase import Client

from ..db.supabase_client import get_supabase_client

async def log_api_usage_to_supabase(
    supabase: Client,
    user_id: Optional[str],
    api_key_id: Optional[str],
    endpoint: str,
    method: str,
    status_code: int,
    response_time_ms: int,
    request_size: Optional[int],
    response_size: Optional[int],
    ip_address: Optional[str],
    user_agent: Optional[str],
):
    """Asynchronously logs API usage data to the Supabase 'api_logs' table."""
    # Skip logging if essential IDs from authenticated requests are missing
    # Allow logging for potentially unauthenticated routes or errors before auth
    # if user_id is None or api_key_id is None:
    #     print("Skipping log: Missing user_id or api_key_id (request might be unauthenticated or failed early)")
    #     return

    log_entry = {
        "user_id": user_id, # May be None
        "api_key_id": api_key_id, # May be None
        "endpoint": endpoint,
        "method": method,
        "status_code": status_code,
        "response_time": response_time_ms, # Changed name to match Go
        "request_size": request_size, # May be None
        "response_size": response_size, # May be None
        "ip_address": ip_address,
        "user_agent": user_agent,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    try:
        # Use await for async Supabase client - REMOVE await for sync client v1
        supabase.table("api_logs").insert(log_entry).execute()
        # print(f"Successfully logged API usage for {endpoint}")
    except Exception as e:
        # Log Supabase errors but don't fail the original request
        print(f"Error logging API usage to Supabase: {e}")

class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        # Get Supabase client during middleware initialization
        # Note: For production, consider dependency injection if client setup becomes complex
        self.supabase_client = get_supabase_client()

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start_time = time.time()

        # Get request details
        method = request.method
        endpoint = request.url.path
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        request_size_str = request.headers.get("content-length")
        request_size = int(request_size_str) if request_size_str and request_size_str.isdigit() else None

        response = None # Initialize response
        try:
            # Process the request and get the response
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            # If an exception occurs during request processing (like a 422)
            # log it and re-raise to let FastAPI handle the error response.
            status_code = 500 # Default or determine from exception if possible
            if isinstance(e, HTTPException):
                 status_code = e.status_code
            
            # Calculate processing time even on error
            process_time = time.time() - start_time
            response_time_ms = int(process_time * 1000)
            
            # Log the error before raising
            await self.log_request(
                request=request,
                status_code=status_code,
                response_time_ms=response_time_ms,
                request_size=request_size,
                response_size=None, # No response size on exception
                ip_address=ip_address,
                user_agent=user_agent
            )
            raise e # Re-raise the exception
        finally:
            # Calculate processing time
            process_time = time.time() - start_time
            response_time_ms = int(process_time * 1000)

            # Get response details if response exists
            response_size = None
            if response:
                response_size_str = response.headers.get("content-length")
                response_size = int(response_size_str) if response_size_str and response_size_str.isdigit() else None

            # Log asynchronously (even if exception occurred, status_code is set)
            await self.log_request(
                request=request,
                status_code=status_code,
                response_time_ms=response_time_ms,
                request_size=request_size,
                response_size=response_size,
                ip_address=ip_address,
                user_agent=user_agent
            )

        return response
    
    async def log_request(
        self,
        request: Request,
        status_code: int,
        response_time_ms: int,
        request_size: Optional[int],
        response_size: Optional[int],
        ip_address: Optional[str],
        user_agent: Optional[str]
    ): 
        """Helper method to structure and send the log."""
        # Get user/API key IDs from request state (set by auth dependency)
        user_id = getattr(request.state, "user_id", None)
        api_key_id = getattr(request.state, "api_key_id", None)

        await log_api_usage_to_supabase(
            supabase=self.supabase_client,
            user_id=user_id,
            api_key_id=api_key_id,
            endpoint=request.url.path,
            method=request.method,
            status_code=status_code,
            response_time_ms=response_time_ms,
            request_size=request_size,
            response_size=response_size,
            ip_address=ip_address,
            user_agent=user_agent,
        ) 