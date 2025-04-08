from typing import Any, Optional, Dict
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Mirroring the structure from response.go for consistency
class APIErrorModel(BaseModel):
    code: str
    message: str
    details: Optional[str] = None

class StandardResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    error: Optional[APIErrorModel] = None

def create_success_response(data: Any, status_code: int = status.HTTP_200_OK) -> JSONResponse:
    """Creates a standardized success JSON response."""
    content = StandardResponse(success=True, data=data).model_dump(exclude_none=True)
    return JSONResponse(status_code=status_code, content=content)

def create_error_response(error: APIErrorModel, status_code: int) -> JSONResponse:
    """Creates a standardized error JSON response.
       Note: For most cases, raising FastAPI's HTTPException is preferred.
       This helper is for cases where we construct the error manually.
    """
    content = StandardResponse(success=False, error=error).model_dump(exclude_none=True)
    return JSONResponse(status_code=status_code, content=content)

# Example usage within an endpoint:
# from .utils.response_utils import create_success_response
# 
# @router.get("/items")
# async def get_items():
#     items_data = [{"id": 1, "name": "Item 1"}]
#     return create_success_response(data=items_data) 