from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from datetime import datetime
from enum import Enum
import uuid
import json

# --- Enums ---
class SchemaType(str, Enum):
    INVOICE = "invoice"
    BANK_STATEMENT = "bank_statement"

# --- Base Model ---
class SchemaBase(BaseModel):
    name: str = Field(..., min_length=3, max_length=255, description="Name of the schema")
    type: SchemaType = Field(..., description="Type of schema (invoice or bank_statement)")
    description: Optional[str] = Field(None, description="Optional description for the schema")
    schema_string: str = Field(..., max_length=102400, description="The schema definition string (max 100KB)") # 100KB limit

    @field_validator('schema_string')
    @classmethod
    def validate_schema_json(cls, v: str):
        try:
            json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(f'Invalid JSON format in schema_string: {e}')
        return v

# --- Request Models ---
class SchemaCreate(SchemaBase):
    pass # Inherits all fields and validators from SchemaBase

class SchemaUpdate(BaseModel):
    # Only description and schema_string can be updated
    description: Optional[str] = Field(None, description="Optional description for the schema")
    schema_string: str = Field(..., max_length=102400, description="The schema definition string (max 100KB)")

    @field_validator('schema_string')
    @classmethod
    def check_schema_string_update(cls, v):
        if not v:
            raise ValueError("schema_string cannot be empty on update")
        # Add JSON validation here too
        try:
            json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(f'Invalid JSON format in schema_string: {e}')
        return v

# --- Response Models (Data Portion) ---
class SchemaResponse(BaseModel):
    id: uuid.UUID = Field(..., description="Unique identifier for the schema")
    name: str = Field(..., description="Name of the schema")
    type: SchemaType = Field(..., description="Type of schema")
    description: Optional[str] = Field(None, description="Optional description")
    version: int = Field(..., description="Schema version number")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        from_attributes = True # Allows creating from ORM models/dicts

class DetailedSchemaResponse(SchemaResponse):
    schema_string: str = Field(..., description="The actual schema definition string")

    class Config:
        from_attributes = True

class ListSchemaResponse(BaseModel):
    schemas: List[SchemaResponse] = Field(..., description="List of schemas")
    total: int = Field(..., description="Total number of schemas matching the query")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")

    class Config:
        from_attributes = True

# --- Model for DB representation (includes user_id, is_active) ---
class SchemaInDB(SchemaBase):
    id: uuid.UUID
    user_id: uuid.UUID
    version: int
    created_at: datetime
    updated_at: datetime
    is_active: bool

    class Config:
        from_attributes = True 