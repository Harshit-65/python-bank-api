import uuid
import logging
from typing import List, Optional, Tuple

from supabase import AsyncClient # Use async client
from postgrest import APIResponse # Import for type hinting
from ..api.models.schema import SchemaCreate, SchemaUpdate, SchemaInDB, SchemaResponse, SchemaType

# --- Initialize Logger ---
logger = logging.getLogger(__name__)

class SchemaNotFoundError(Exception):
    pass

class SchemaExistsError(Exception):
    pass

class DatabaseOperationError(Exception):
    pass

async def check_existing_schema(supabase: AsyncClient, user_id: uuid.UUID, name: str, exclude_id: Optional[uuid.UUID] = None) -> bool:
    """Checks if an active schema with the same name exists for the user, optionally excluding one ID."""
    query = supabase.table("schemas").select("id", count="exact").eq("user_id", str(user_id)).eq("name", name).eq("is_active", True)
    if exclude_id:
        query = query.not_("id", "eq", str(exclude_id))
    
    try:
        response: APIResponse = await query.execute()
        return response.count > 0
    except Exception as e:
        logger.error(f"SCHEMA DB: Error checking existing schema for user {user_id}, name {name}: {e}")
        raise DatabaseOperationError(f"Error checking existing schema: {e}")

async def create_schema(supabase: AsyncClient, schema_in: SchemaCreate, user_id: uuid.UUID) -> SchemaInDB:
    """Creates a new schema in the database."""
    # Check for existing active schema with the same name
    if await check_existing_schema(supabase, user_id, schema_in.name):
        raise SchemaExistsError(f"Active schema with name '{schema_in.name}' already exists.")

    insert_data = {
        "user_id": str(user_id),
        "name": schema_in.name,
        "type": schema_in.type.value,
        "description": schema_in.description,
        "schema_string": schema_in.schema_string,
        "version": 1,
        "is_active": True,
    }
    try:
        response: APIResponse = await supabase.table("schemas").insert(insert_data, returning="representation").execute()
        
        if not response.data or len(response.data) == 0:
            logger.error(f"SCHEMA DB: Failed to create schema for user {user_id}, name {schema_in.name}. Response: {response}")
            raise DatabaseOperationError("Failed to create schema or retrieve created record.")
            
        # Assuming the first item in data is the created schema
        return SchemaInDB(**response.data[0])
    except Exception as e:
        logger.error(f"SCHEMA DB: Error creating schema for user {user_id}, name {schema_in.name}: {e}")
        raise DatabaseOperationError(f"Error creating schema: {e}")

async def get_schema(supabase: AsyncClient, schema_id: uuid.UUID, user_id: uuid.UUID) -> SchemaInDB:
    """Retrieves a single active schema by ID and user ID."""
    try:
        response: APIResponse = await supabase.table("schemas").select("*").eq("id", str(schema_id)).eq("user_id", str(user_id)).eq("is_active", True).maybe_single().execute()
        
        if response.data is None:
            raise SchemaNotFoundError(f"Schema with ID {schema_id} not found for user.")
            
        return SchemaInDB(**response.data)
    except Exception as e:
        logger.error(f"SCHEMA DB: Error fetching schema {schema_id} for user {user_id}: {e}")
        if isinstance(e, SchemaNotFoundError): raise # Re-raise specific error
        raise DatabaseOperationError(f"Error fetching schema: {e}")

async def list_schemas(
    supabase: AsyncClient, 
    user_id: uuid.UUID, 
    page: int, 
    page_size: int, 
    schema_type: Optional[SchemaType] = None
) -> Tuple[List[SchemaInDB], int]:
    """Retrieves a paginated list of active schemas for a user."""
    offset = (page - 1) * page_size
    query = supabase.table("schemas").select("*", count="exact").eq("user_id", str(user_id)).eq("is_active", True).order("created_at", desc=True).range(offset, offset + page_size - 1)

    if schema_type:
        query = query.eq("type", schema_type.value)
        
    try:
        response: APIResponse = await query.execute()
        schemas = [SchemaInDB(**item) for item in response.data]
        total_count = response.count if response.count is not None else 0
        return schemas, total_count
    except Exception as e:
        logger.error(f"SCHEMA DB: Error listing schemas for user {user_id}: {e}")
        raise DatabaseOperationError(f"Error listing schemas: {e}")

async def update_schema(supabase: AsyncClient, schema_id: uuid.UUID, schema_in: SchemaUpdate, user_id: uuid.UUID) -> SchemaInDB:
    """Updates an existing schema (description, schema_string) and increments version."""
    # Fetch existing schema to get current version and check ownership/existence
    existing_schema = await get_schema(supabase, schema_id, user_id) # Handles NotFound

    update_data = {
        "description": schema_in.description,
        "schema_string": schema_in.schema_string,
        "version": existing_schema.version + 1,
        "updated_at": datetime.now(timezone.utc).isoformat() # Explicitly set updated_at
    }
    
    try:
        response: APIResponse = await supabase.table("schemas").update(update_data, returning="representation").eq("id", str(schema_id)).eq("user_id", str(user_id)).execute()
        
        if not response.data or len(response.data) == 0:
            logger.error(f"SCHEMA DB: Failed to update schema {schema_id} for user {user_id}. Response: {response}")
            raise DatabaseOperationError("Failed to update schema or retrieve updated record.")
            
        return SchemaInDB(**response.data[0])
    except Exception as e:
        logger.error(f"SCHEMA DB: Error updating schema {schema_id} for user {user_id}: {e}")
        raise DatabaseOperationError(f"Error updating schema: {e}")

async def delete_schema(supabase: AsyncClient, schema_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    """Soft deletes a schema by setting is_active to False."""
    # Check if schema exists and belongs to user first (optional but good practice)
    await get_schema(supabase, schema_id, user_id) # Will raise NotFoundError if not found
    
    update_data = {
        "is_active": False,
        "updated_at": datetime.now(timezone.utc).isoformat() 
    }
    try:
        response: APIResponse = await supabase.table("schemas").update(update_data).eq("id", str(schema_id)).eq("user_id", str(user_id)).execute()
        # Check if update affected any rows (though maybe_single in get_schema covers existence)
        # If response.data is empty/None, it means the row wasn't found or didn't match RLS
        # Note: Supabase update might not return data unless returning='representation' is used
        # We rely on get_schema for existence check. Success here means the update query ran.
        return True 
    except Exception as e:
        logger.error(f"SCHEMA DB: Error deleting schema {schema_id} for user {user_id}: {e}")
        raise DatabaseOperationError(f"Error deleting schema: {e}") 