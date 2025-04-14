import uuid
from typing import List, Optional, Annotated

from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from supabase import AsyncClient # Use async client

# Internal imports
from ...auth.dependencies import AuthData
from ...db.supabase_client import get_supabase_client # Use async client
from ..models.schema import (
    SchemaType, SchemaCreate, SchemaUpdate, 
    SchemaResponse, DetailedSchemaResponse, ListSchemaResponse
)
from ..models.requests import PaginationParams # Re-use pagination
from ...utils.schema import (
    create_schema, get_schema, list_schemas, update_schema, delete_schema,
    SchemaNotFoundError, SchemaExistsError, DatabaseOperationError
)
from ..utils.response_utils import create_success_response

# --- Constants ---
MAX_SCHEMAS_PER_USER = 50

router = APIRouter(
    prefix="/schemas",
    tags=["Schema Management"],
)

# --- Helper for UUID validation ---
def validate_uuid(schema_id_str: str) -> uuid.UUID:
    try:
        return uuid.UUID(schema_id_str)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Schema ID format.")

# --- Endpoints ---

@router.post("", 
             response_model=SchemaResponse, 
             status_code=status.HTTP_201_CREATED, 
             summary="Create Schema",
             description="Creates a new custom output schema for the authenticated user.")
async def handle_create_schema(
    schema_in: SchemaCreate,
    auth: AuthData, # Inject auth data
    supabase: Annotated[AsyncClient, Depends(get_supabase_client)]
):
    user_id = uuid.UUID(auth["user_id"])
    try:
        # --- Check Max Schemas Limit ---
        _, current_count = await list_schemas(supabase, user_id, 1, 0) # page=1, page_size=0 to get count only
        if current_count >= MAX_SCHEMAS_PER_USER:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail=f"Schema limit reached ({MAX_SCHEMAS_PER_USER}). Please delete unused schemas."
            )
        # --- End Check ---

        created_schema = await create_schema(supabase, schema_in, user_id)
        # Return SchemaResponse (without schema_string)
        return SchemaResponse.model_validate(created_schema)
    except SchemaExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except DatabaseOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except HTTPException as e: # Re-raise existing HTTP exceptions
        raise e
    except Exception as e:
        # Catch any other unexpected errors
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.get("/list", 
            response_model=ListSchemaResponse,
            summary="List Schemas",
            description="Lists all active schemas for the authenticated user, with optional type filtering and pagination.")
async def handle_list_schemas(
    auth: AuthData,
    supabase: Annotated[AsyncClient, Depends(get_supabase_client)],
    pagination: Annotated[PaginationParams, Depends()], # Re-use pagination model
    type: Optional[SchemaType] = Query(None, description="Filter schemas by type (invoice or bank_statement)")
):
    user_id = uuid.UUID(auth["user_id"])
    try:
        schemas_in_db, total_count = await list_schemas(supabase, user_id, pagination.page, pagination.page_size, type)
        
        # Convert DB models to response models
        schema_responses = [SchemaResponse.model_validate(s) for s in schemas_in_db]
        
        return ListSchemaResponse(
            schemas=schema_responses,
            total=total_count,
            page=pagination.page,
            page_size=pagination.page_size
        )
    except DatabaseOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.get("/id/{schema_id_str}", 
            response_model=DetailedSchemaResponse,
            summary="Get Schema",
            description="Retrieves the details of a specific active schema, including the schema string.")
async def handle_get_schema(
    schema_id_str: Annotated[str, Path(description="The UUID of the schema to retrieve.")],
    auth: AuthData,
    supabase: Annotated[AsyncClient, Depends(get_supabase_client)]
):
    user_id = uuid.UUID(auth["user_id"])
    schema_id = validate_uuid(schema_id_str) # Validate and convert path param
    try:
        schema_in_db = await get_schema(supabase, schema_id, user_id)
        return DetailedSchemaResponse.model_validate(schema_in_db)
    except SchemaNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found.")
    except DatabaseOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.put("/id/{schema_id_str}", 
           response_model=DetailedSchemaResponse,
           summary="Update Schema",
           description="Updates the description and schema_string of an existing schema. Increments the version number.")
async def handle_update_schema(
    schema_id_str: Annotated[str, Path(description="The UUID of the schema to update.")],
    schema_in: SchemaUpdate,
    auth: AuthData,
    supabase: Annotated[AsyncClient, Depends(get_supabase_client)]
):
    user_id = uuid.UUID(auth["user_id"])
    schema_id = validate_uuid(schema_id_str)
    try:
        updated_schema = await update_schema(supabase, schema_id, schema_in, user_id)
        return DetailedSchemaResponse.model_validate(updated_schema)
    except SchemaNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found.")
    except DatabaseOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.delete("/id/{schema_id_str}", 
              status_code=status.HTTP_204_NO_CONTENT,
              summary="Delete Schema",
              description="Soft deletes a schema by marking it as inactive.")
async def handle_delete_schema(
    schema_id_str: Annotated[str, Path(description="The UUID of the schema to delete.")],
    auth: AuthData,
    supabase: Annotated[AsyncClient, Depends(get_supabase_client)]
):
    user_id = uuid.UUID(auth["user_id"])
    schema_id = validate_uuid(schema_id_str)
    try:
        success = await delete_schema(supabase, schema_id, user_id)
        if not success: # Should technically be caught by get_schema in delete_schema
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found.")
        # No content response on success
        return
    except SchemaNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found.")
    except DatabaseOperationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}") 