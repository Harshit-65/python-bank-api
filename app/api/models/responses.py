from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, AnyHttpUrl
from datetime import datetime

from .requests import JobStatus # Import JobStatus enum

# --- Response Models (Data Portion) ---
# These models represent the 'data' field in a successful StandardResponse

class UploadedFileModel(BaseModel):
    file_id: str
    file_url: Optional[str] = None # Changed from AnyHttpUrl to str to fix JSON serialization
    doc_id: Optional[str] = None
    error: Optional[str] = None
    type: Optional[str] = None

class UploadResponseModel(BaseModel):
    files: List[UploadedFileModel]
    status: str # e.g., "done", "partial_error"

class JobCreatedResponseModel(BaseModel):
    job_id: str
    status: JobStatus
    created_at: datetime
    updated_at: datetime

class JobStatusResponseModel(BaseModel):
    job_id: str
    status: JobStatus
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime
    # Add other relevant fields if available from DB, e.g., file_id
    file_id: Optional[str] = None

class BatchFileResponseModel(BaseModel):
    file_id: str = Field(..., description="The ID of the file")
    document_id: Optional[str] = Field(None, description="The ID of the document (if different from file_id)")
    status: JobStatus = Field(..., description="The status of the file processing")
    error: Optional[str] = Field(None, description="Error message if processing failed")

class BatchCreatedResponseModel(BaseModel):
    batch_id: str = Field(..., description="The ID of the batch job")
    status: JobStatus = Field(..., description="The status of the batch job")
    total_files: int = Field(..., description="Total number of files in the batch")
    created_at: datetime = Field(..., description="When the batch was created")
    callback_url: Optional[str] = Field(None, description="Callback URL for batch completion notification")

class BatchStatusResponseModel(BaseModel):
    batch_id: str = Field(..., description="The ID of the batch job")
    status: JobStatus = Field(..., description="The status of the batch job")
    total_files: int = Field(..., description="Total number of files in the batch")
    processed_files: int = Field(..., description="Number of files processed so far")
    failed_files: int = Field(..., description="Number of files that failed processing")
    created_at: datetime = Field(..., description="When the batch was created")
    updated_at: datetime = Field(..., description="When the batch was last updated")
    files: List[BatchFileResponseModel] = Field(..., description="List of files in the batch with their status")
    callback_url: Optional[str] = Field(None, description="Callback URL for batch completion notification")

class JobResponseModel(BaseModel):
    job_id: str = Field(..., description="The ID of the job")
    status: JobStatus = Field(..., description="The status of the job")
    created_at: datetime = Field(..., description="When the job was created")
    updated_at: datetime = Field(..., description="When the job was last updated")
    result: Optional[Dict[str, Any]] = Field(None, description="The result of the job if completed")
    error: Optional[str] = Field(None, description="Error message if job failed")

class JobListResponseModel(BaseModel):
    jobs: List[JobResponseModel] = Field(..., description="List of jobs")
    total: int = Field(..., description="Total number of jobs")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")

class HealthStatusModel(BaseModel):
    status: bool
    message: Optional[str] = None

class ServicesHealthModel(BaseModel):
    database: HealthStatusModel
    # Add other services like Queue, AI Service later
    # queue: HealthStatusModel
    # ai_service: HealthStatusModel

class HealthCheckResponseModel(BaseModel):
    status: str = "ok"
    timestamp: datetime
    services: ServicesHealthModel

# --- Pagination Model (Data Portion) ---

class PaginatedJobResponse(BaseModel):
    jobs: List[JobStatusResponseModel] # List of job statuses
    total: int
    page: int
    page_size: int
    total_pages: int

# Note: The top-level response structure (success, data, error)
# is handled by the StandardResponse in response_utils.py
# These models define what goes into the 'data' field. 