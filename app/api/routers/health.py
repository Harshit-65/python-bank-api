from fastapi import APIRouter, Depends, status
from datetime import datetime, timezone
from supabase import Client

from ...db.supabase_client import get_supabase_client
from ..models.responses import HealthCheckResponseModel, ServicesHealthModel, HealthStatusModel
from ..utils.response_utils import create_success_response # Use our standard response

router = APIRouter(
    prefix="/health",
    tags=["Health"],
)

async def check_db_health(supabase: Client = Depends(get_supabase_client)) -> HealthStatusModel:
    """Checks connectivity to the database."""
    try:
        # Simple check: Try to fetch a single item from a known small table or schema info
        # Adjust table/query as needed for your Supabase setup
        await supabase.table("api_keys").select("id").limit(1).execute()
        return HealthStatusModel(status=True)
    except Exception as e:
        print(f"Database health check failed: {e}")
        return HealthStatusModel(status=False, message=str(e))

@router.get("", 
            response_model=HealthCheckResponseModel, # Defines the structure of the *data* field
            summary="Perform Health Check",
            description="Checks the status of the service and its dependencies (e.g., Database).")
async def health_check(db_health: HealthStatusModel = Depends(check_db_health)) -> HealthCheckResponseModel:
    """Returns the current health status of the API and connected services."""
    services_health = ServicesHealthModel(database=db_health)
    health_data = HealthCheckResponseModel(
        timestamp=datetime.now(timezone.utc),
        services=services_health
    )
    # Although we define response_model, FastAPI doesn't automatically wrap it
    # in our {success: true, data: ...} structure. So we return the model directly.
    # If we wanted the wrapper, we'd use `create_success_response(data=health_data)`
    # but then the response_model in the decorator might be slightly misleading.
    # For health checks, returning the direct model is common.
    return health_data 