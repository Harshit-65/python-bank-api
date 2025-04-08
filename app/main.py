from fastapi import FastAPI

from .middleware.logging import LoggingMiddleware
from .auth.dependencies import AuthData # Example import
from .api.routers import health, upload, jobs # <-- Import new routers
from .db.supabase_client import get_supabase_client

# Create FastAPI app instance
app = FastAPI(
    title="Python Bank API",
    description="Migrated Bank API service",
    version="1.0.0",
    # Add OpenAPI tags metadata for better documentation
    openapi_tags=[
        {"name": "Health", "description": "Service health checks"},
        {"name": "Upload", "description": "File upload operations"},
        {"name": "Parsing Jobs", "description": "Operations for submitting and managing parsing jobs"},
    ]
)

# Add Middleware
# LoggingMiddleware should typically be one of the first middleware added
supabase = get_supabase_client()
app.add_middleware(LoggingMiddleware, supabase=supabase)

# --- Add Routers ---
# It's often good practice to prefix API routes with /api/v1 or similar
API_PREFIX = "/api/v1"

app.include_router(health.router, prefix=API_PREFIX, tags=["Health"]) # Unauthenticated usually
app.include_router(upload.router, prefix=API_PREFIX + "/parse", tags=["Upload"]) # Requires auth
app.include_router(jobs.router, prefix=API_PREFIX + "/parse", tags=["Parsing Jobs"]) # Requires auth

# --- Root Endpoint (Optional) ---
@app.get("/", include_in_schema=False)
async def root():
    return {"message": "Welcome to the Python Bank API"}


# --- Example Protected Route (Keep or remove) ---
# @app.get(API_PREFIX + "/items/") # Add prefix
# async def read_items(auth: AuthData): 
#     """Example endpoint protected by authentication."""
#     return create_success_response(data=[{"item_id": "Foo", "user": auth["user_id"]}])

# Add routers later, e.g.:
# from .api import items_router
# app.include_router(items_router, prefix="/api/v1") 