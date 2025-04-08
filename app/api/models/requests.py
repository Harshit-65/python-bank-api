from typing import List, Optional
from enum import Enum
from pydantic import BaseModel, Field, field_validator, AnyHttpUrl
from fastapi import UploadFile, Form, File, HTTPException, status

MAX_FILES = 50
MAX_FILE_SIZE_MB = 10
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
ALLOWED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png"}

# --- Enums ---
class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

# --- Request Models ---

class BatchFileModel(BaseModel):
    file_id: str = Field(..., description="The ID of the file to process")
    document_id: Optional[str] = Field(None, description="The ID of the document (if different from file_id)")

    @field_validator('file_id')
    @classmethod
    def validate_file_id(cls, v: str):
        if not v:
            raise ValueError("file_id is required")
        return v

class BatchRequestModel(BaseModel):
    files: List[BatchFileModel] = Field(..., max_length=MAX_FILES, description="List of files to process in batch")
    callback_url: Optional[str] = Field(None, description="Optional callback URL for batch completion notification")

    @field_validator('files')
    @classmethod
    def check_min_files(cls, v: list):
        if not v:
            raise ValueError("At least one file is required")
        return v

class ParseRequestModel(BaseModel):
    file_id: str = Field(..., min_length=1)
    document_id: str = Field(..., min_length=1)
    callback_url: Optional[str] = None

# --- Models for Query Parameters (used in List requests) ---

class PaginationParams(BaseModel):
    page: int = Field(1, gt=0)
    page_size: int = Field(20, gt=0, le=100)
    status: Optional[JobStatus] = None

# Note: FileUpload validation is handled directly in the endpoint using Depends/File 