from contextlib import asynccontextmanager
from fastapi import FastAPI
from .middleware.logging import LoggingMiddleware
from .api.routers import health, upload, jobs
from .db.supabase_client import get_supabase_client
from .queue.arq_client import init_arq_pool, close_arq_pool

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize dependencies
    print("Executing in: main.py - lifespan start")
    await init_arq_pool()
    supabase = get_supabase_client()
    
    yield  # Application runs here
    
    # Cleanup
    print("Executing in: main.py - lifespan end")
    await close_arq_pool()
    await supabase.auth.close()

app = FastAPI(
    title="Python Bank API",
    description="Migrated Bank API service",
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Health", "description": "Service health checks"},
        {"name": "Upload", "description": "File upload operations"},
        {"name": "Parsing Jobs", "description": "Operations for submitting and managing parsing jobs"},
    ]
)

# Add middleware
print("Executing in: main.py - Registering Middleware")
app.add_middleware(LoggingMiddleware, supabase=get_supabase_client())

# Configure routes
print("Executing in: main.py - Including Routers")
API_PREFIX = "/api/v1"
app.include_router(health.router, prefix=API_PREFIX, tags=["Health"])
app.include_router(upload.router, prefix=API_PREFIX + "/parse", tags=["Upload"])
app.include_router(jobs.router, prefix=API_PREFIX + "/parse", tags=["Parsing Jobs"])

# @app.get("/", include_in_schema=False)
# async def root():
#     return {"message": "Welcome to the Python Bank API"}