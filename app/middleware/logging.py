import time
import datetime

from fastapi import HTTPException
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from supabase import Client

from ..db.supabase_client import get_supabase_client

async def log_api_usage_to_supabase(
    supabase: Client,
    user_id: str,
    api_key_id: str,
    endpoint: str,
    method: str,
    status_code: int,
    response_time: float,
    request_size: Optional[int] = None,
    response_size: Optional[int] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> None:
    """
    Log API usage to Supabase.
    
    Args:
        supabase: Supabase client
        user_id: User ID
        api_key_id: API key ID
        endpoint: API endpoint
        method: HTTP method
        status_code: HTTP status code
        response_time: Response time in milliseconds
        request_size: Request size in bytes (optional)
        response_size: Response size in bytes (optional)
        ip_address: IP address (optional)
        user_agent: User agent (optional)
    """
    try:
        # Skip logging if user_id or api_key_id is null
        if user_id is None or api_key_id is None:
            print("Skipping log: Missing user_id or api_key_id")
            return

        # Prepare log data
        log_data = {
            "user_id": user_id,
            "api_key_id": api_key_id,
            "endpoint": endpoint,
            "method": method,
            "status_code": status_code,
            "response_time": int(response_time),
            "request_size": request_size,
            "response_size": response_size,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "created_at": datetime.datetime.now().isoformat()
        }

        # Insert log into Supabase
        result = supabase.table("api_logs").insert(log_data).execute()
        
        if hasattr(result, "error") and result.error:
            print(f"Error logging API usage: {result.error}")
    except Exception as e:
        print(f"Error logging API usage: {str(e)}")

class LoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, supabase: Client):
        super().__init__(app)
        self.supabase = supabase

    async def dispatch(self, request: Request, call_next):
        # Start timer
        start_time = time.time()
        
        # Get request details
        method = request.method
        endpoint = request.url.path
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        
        # Get request size if available
        request_size = None
        if hasattr(request, "_body"):
            request_size = len(request._body)
        
        # Process request
        response = await call_next(request)
        
        # Calculate response time
        response_time = int((time.time() - start_time) * 1000)  # Convert to milliseconds
        
        # Get response size if available
        response_size = None
        if hasattr(response, "body"):
            response_size = len(response.body)
        
        # Get user_id and api_key_id from request state if available
        user_id = getattr(request.state, "user_id", None)
        api_key_id = getattr(request.state, "api_key_id", None)
        
        # Skip logging if user_id or api_key_id is null
        if user_id is None or api_key_id is None:
            print(f"Skipping log for {method} {endpoint}: Missing user_id or api_key_id")
            return response
        
        # Log API usage
        await log_api_usage_to_supabase(
            supabase=self.supabase,
            user_id=user_id,
            api_key_id=api_key_id,
            endpoint=endpoint,
            method=method,
            status_code=response.status_code,
            response_time=response_time,
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
        response_time: int,
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
            supabase=self.supabase,
            user_id=user_id,
            api_key_id=api_key_id,
            endpoint=request.url.path,
            method=request.method,
            status_code=status_code,
            response_time=response_time,
            request_size=request_size,
            response_size=response_size,
            ip_address=ip_address,
            user_agent=user_agent,
        ) 