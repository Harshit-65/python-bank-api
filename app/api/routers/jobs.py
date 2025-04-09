from typing import List, Optional, Annotated
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from supabase import Client
import uuid
import math
from datetime import datetime, timezone
from arq.connections import ArqRedis
import json
import redis # Import redis library
from ...config import REDIS_URL, REDIS_PASSWORD # Import redis config
import logging # Import logging

# Get logger instance
logger = logging.getLogger(__name__)

from ...db.supabase_client import get_supabase_client
from ...auth.dependencies import AuthData
from ...queue.arq_client import get_arq_redis, enqueue_job
from ..models.requests import ParseRequestModel, PaginationParams, JobStatus, BatchRequestModel
from ..models.responses import (
    JobCreatedResponseModel, JobStatusResponseModel, 
    PaginatedJobResponse, BatchCreatedResponseModel, 
    BatchStatusResponseModel, JobResponseModel, BatchFileResponseModel # Added BatchFileResponseModel
)
from ..utils.response_utils import create_success_response, create_error_response, APIErrorModel # Import error utils

# Helper to get sync redis client (could be moved to a shared utils)
def get_sync_redis_client_for_api() -> redis.Redis:
    """Creates a synchronous Redis client for API endpoints."""
    return redis.from_url(REDIS_URL, password=REDIS_PASSWORD, decode_responses=True)

async def get_job_from_db(supabase: Client, job_id: str, user_id: str) -> Optional[dict]:
    """Fetches job status from the DB."""
    try:
        response = await supabase.table("jobs").select("*, documents(file_name)").eq("id", job_id).eq("user_id", user_id).maybe_single().execute()
        return response.data
    except Exception as e:
        print(f"Error fetching job {job_id}: {e}")
        return None

async def list_jobs_from_db(supabase: Client, user_id: str, job_type: str, params: PaginationParams) -> (List[dict], int):
    """Lists jobs from the DB with pagination."""
    offset = (params.page - 1) * params.page_size
    query = supabase.table("jobs").select("*, documents(file_name)", count="exact").eq("user_id", user_id).eq("job_type", job_type)
    if params.status:
        query = query.eq("status", params.status.value)
    
    query = query.order("created_at", desc=True).range(offset, offset + params.page_size - 1)

    try:
        response = await query.execute()
        jobs = response.data or []
        total_count = response.count or 0
        return jobs, total_count
    except Exception as e:
        print(f"Error listing jobs: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to list jobs")

# --- Batch Processing Functions (DB interaction for batch metadata) ---
async def get_batch_job_record(supabase: Client, batch_id: str, user_id: str) -> Optional[dict]:
    """Retrieves a batch job from the database."""
    try:
        response = await supabase.table("batch_jobs").select("*").eq("id", batch_id).eq("user_id", user_id).maybe_single().execute()
        return response.data
    except Exception as e:
        print(f"Error fetching batch job {batch_id}: {e}")
        return None

# --- Router --- 

router = APIRouter(
    tags=["Parsing Jobs"],
    # dependencies=[Depends(AuthData)], # <-- Commented out router-level dependency
)

# --- Helper to Verify Document --- 
async def verify_document_access(supabase: Client, doc_id: str, user_id: str):
    """Verify the user owns the document and it exists."""
    print(f"Executing in: jobs.py - verify_document_access for doc_id: {doc_id}")
    try:
        doc_response = supabase.table("documents").select("id, document_type").eq("id", doc_id).eq("user_id", user_id).maybe_single().execute()
        if not doc_response.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Document with ID '{doc_id}' not found or access denied.")
        return doc_response.data # Return doc info (including type)
    except HTTPException: # Re-raise HTTPException
        raise
    except Exception as e:
        print(f"Error verifying document {doc_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error verifying document")

# --- General Parse Endpoint ---
# @router.post("", 
#               response_model=JobCreatedResponseModel, 
#               status_code=status.HTTP_202_ACCEPTED, 
#               summary="Submit Document for Parsing",
#               description="General endpoint that routes to the appropriate handler based on document type.")
# async def general_parse(
#     request_data: ParseRequestModel,
#     auth: AuthData,
#     supabase: Annotated[Client, Depends(get_supabase_client)],
#     arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
# ):
#     \"\"\"General parse endpoint that routes to the appropriate handler based on document type.\"\"\"
#     user_id = auth["user_id"]
#     doc_id = request_data.file_id # Use file_id from request as document_id
    
#     # Get document details to determine document type
#     try:
#         doc_response = await supabase.table("documents").select("document_type").eq("id", doc_id).eq("user_id", user_id).maybe_single().execute()
#         if not doc_response.data:
#             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Document with ID '{doc_id}' not found or access denied.")
        
#         doc_type = doc_response.data.get("document_type")
#         if doc_type == "bank_statement":
#             # Route to bank statement handler
#             return await submit_statement_job(request_data, auth, supabase, arq_redis)
#         elif doc_type == "invoice":
#             # Route to invoice handler
#             return await submit_invoice_job(request_data, auth, supabase, arq_redis)
#         else:
#             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported document type: {doc_type}")
#     except Exception as e:
#         print(f"Error routing document {doc_id}: {e}")
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error routing document: {e}")

# --- Statement Endpoints ---
@router.post("/statements", 
              response_model=JobCreatedResponseModel, # <-- Uncommented
              status_code=status.HTTP_202_ACCEPTED, 
              summary="Submit Bank Statement for Parsing",
              description="Submits a bank statement (previously uploaded) for parsing.")
async def submit_statement_job(
    request_data: ParseRequestModel, # <-- Changed back to ParseRequestModel
    auth: AuthData, # <-- Uncommented
    supabase: Annotated[Client, Depends(get_supabase_client)], # <-- Uncommented
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)] # <-- Uncommented
):
    """Submit a bank statement for parsing."""
    # --- Print statement removed --- 
    # print(f"Executing in: jobs.py - submit_statement_job - AFTER validation for data: {request_data}")
    
    # --- Original logic restored --- 
    user_id = auth["user_id"]
    doc_id = request_data.document_id # Use document_id from request

    # 1. Verify document exists and belongs to user
    doc_info = await verify_document_access(supabase, doc_id, user_id)
    if doc_info.get("document_type") != "bank_statement":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Document {doc_id} is not a bank statement.")

    # 2. Create Job Record
    job_id = f"job_{uuid.uuid4()}"
    # --- REMOVE THE DATABASE INSERT ---
    # job_record = {
    #     "id": job_id,
    #     "document_id": doc_id,
    #     "file_id": request_data.file_id,
    #     "status": JobStatus.PENDING.value,
    #     "callback_url": str(request_data.callback_url) if request_data.callback_url else None,
    #     "job_type": "bank_statement", 
    #     "user_id": user_id,
    #     "created_at": datetime.now(timezone.utc).isoformat(),
    #     "updated_at": datetime.now(timezone.utc).isoformat(),
    # }
    # # Log the exact record being inserted
    # print(f"Attempting to insert job record: {json.dumps(job_record, indent=2)}")
    # try:
    #     response = supabase.table("jobs").insert(job_record).execute()
    #     if hasattr(response, 'error') and response.error:
    #          raise Exception(f"Supabase API error: {response.error}")
    # except Exception as e:
    #     print(f"Error type creating job record: {type(e)}")
    #     print(f"Error repr creating job record: {repr(e)}")
    #     print(f"Error creating job record for doc {doc_id}: {e}")
    #     # Consider rolling back or cleaning up if needed
    #     raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create job record")

    # 3. Enqueue the ARQ Task with all necessary job details
    try:
        ctx = {"arq_redis": arq_redis}
        # Pass all necessary info for the worker to create the job record
        job_params = {
            "job_id": job_id, 
            "document_id": doc_id,
            "file_id": request_data.file_id, # Pass file_id
            "user_id": user_id, # Pass user_id
            "callback_url": str(request_data.callback_url) if request_data.callback_url else None, # Pass callback_url
            "job_type": "bank_statement" # Pass job_type
        }
        await enqueue_job(ctx, "process_document", job_params)
        print(f"Enqueued job {job_id} with params: {job_params}") # Log enqueued params
    except Exception as e:
        # If enqueue fails, update DB job status to FAILED
        # NOTE: We can't update the job status here anymore since it doesn't exist yet.
        # Log the enqueue failure. The worker won't pick it up.
        print(f"Fatal error: Failed to enqueue job {job_id}: {e}")
        # Consider how to handle this - maybe return 500 immediately?
        # For now, raising the 500 seems appropriate as the job cannot be processed.
        # print(f"Error enqueuing job {job_id}: {e}. Updating job status to failed.")
        # try:
        #     await supabase.table("jobs").update({"status": JobStatus.FAILED.value, "error_message": "Enqueue failed"}).eq("id", job_id).execute()
        # except Exception as db_err:
        #     print(f"Error updating job {job_id} status to failed after enqueue error: {db_err}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to enqueue job: {e}")

    # 4. Return Job ID and Accepted status
    now = datetime.now(timezone.utc)
    response_data = JobCreatedResponseModel(
        job_id=job_id, 
        status=JobStatus.PENDING,
        created_at=now, # Include current time
        updated_at=now  # Include current time
        )
    # Use mode='json' to serialize datetime objects correctly
    return create_success_response(data=response_data.model_dump(mode='json'), status_code=status.HTTP_202_ACCEPTED)
    # return {"test_received": request_data.test, "status": "test passed validation - dependencies removed"} # <-- Removed test return

@router.get("/statements/{job_id}", 
             response_model=JobResponseModel, 
             summary="Get Bank Statement Job Status",
             description="Retrieves the status and results (if available) of a specific bank statement parsing job from Redis.")
async def get_statement_job_status(
    job_id: Annotated[str, Path(description="The ID of the job to retrieve.")],
    auth: AuthData, # Keep auth to check ownership if needed
    # supabase: Annotated[Client, Depends(get_supabase_client)], # No longer needed for primary fetch
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)] # Inject Redis client
):
    """Get the status of a bank statement parsing job from Redis."""
    user_id = auth["user_id"]
    redis_key = f"job:{job_id}"

    try:
        job_json = redis_client.get(redis_key)
        if not job_json:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with ID '{job_id}' not found.")

        job_data = json.loads(job_json)
        
        # Optional: Verify ownership (important if user_id is part of job_data in Redis)
        if job_data.get("user_id") != user_id:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this job.")

        # Parse results (already parsed in job_data if stored correctly)
        # results_dict = job_data.get("result") # Already a dict/list/etc.
        
        # Ensure timestamps are converted if needed (Redis stores strings)
        # Pydantic should handle string -> datetime conversion in the response model
        created_at_str = job_data.get("created_at")
        updated_at_str = job_data.get("updated_at")

        # Construct response using JobResponseModel
        # Pydantic will validate and convert types
        response_data = JobResponseModel(
            job_id=job_data.get("id", job_id),
            status=JobStatus(job_data.get("status", JobStatus.FAILED)), # Default to FAILED if missing 
            result=job_data.get("result"), # Use result directly
            error=job_data.get("error_message"),
            created_at=created_at_str, # Pass string, Pydantic converts
            updated_at=updated_at_str, # Pass string, Pydantic converts
        )
        # Use mode='json' to handle datetime serialization
        return create_success_response(data=response_data.model_dump(mode='json', exclude_none=True))

    except json.JSONDecodeError:
         logger.error(f"Job {job_id}: Failed to decode job JSON from Redis: {job_json}")
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to parse job data.")
    except redis.RedisError as e:
        logger.error(f"Job {job_id}: Redis error fetching job: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve job status.")
    except Exception as e:
        logger.error(f"Job {job_id}: Unexpected error fetching job status: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error.")

# --- NEW --- List Statement Jobs Endpoint ---
@router.get("/statements", 
             response_model=PaginatedJobResponse, 
             summary="List Bank Statement Jobs",
             description="Lists bank statement parsing jobs for the authenticated user, fetching from Redis with optional status filtering and pagination.")
async def list_statement_jobs(
    auth: AuthData,
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)],
    pagination: Annotated[PaginationParams, Depends()], 
):
    """List all bank statement parsing jobs from Redis."""
    user_id = auth["user_id"]
    job_type_filter = "bank_statement"
    
    filtered_jobs = []
    try:
        # Iterate through all job keys in Redis
        for key in redis_client.scan_iter("job:*"):
            job_json = redis_client.get(key)
            if not job_json:
                continue
            
            try:
                job_data = json.loads(job_json)
            except json.JSONDecodeError:
                logger.warning(f"Failed to decode JSON for key {key}. Skipping.")
                continue

            # --- Apply Filters ---
            # 1. Filter by User ID (Essential Security)
            if job_data.get("user_id") != user_id:
                continue
            # 2. Filter by Job Type
            if job_data.get("job_type") != job_type_filter:
                continue
            # 3. Filter by Status (if provided)
            if pagination.status and job_data.get("status") != pagination.status.value:
                continue
            
            # Add job to list if all filters pass
            filtered_jobs.append(job_data)
            
    except redis.RedisError as e:
        logger.error(f"Redis error listing jobs for user {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to list jobs due to cache error.")

    # --- Sort by creation date (descending) ---
    # Use isoformat string comparison (should work for standard format)
    filtered_jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
    
    # --- Apply Pagination ---
    total_count = len(filtered_jobs)
    start_index = (pagination.page - 1) * pagination.page_size
    end_index = start_index + pagination.page_size
    paginated_job_data = filtered_jobs[start_index:end_index]
    
    # --- Format Response Items ---
    job_responses = []
    for job in paginated_job_data:
        # Use JobStatusResponseModel for list items (as previously defined)
        job_responses.append(
            JobStatusResponseModel(
                job_id=job.get("id", "unknown"),
                status=JobStatus(job.get("status", JobStatus.FAILED)),
                # Results are generally not included in list views for brevity
                # result=job.get("result"), 
                error_message=job.get("error_message"),
                created_at=job.get("created_at"),
                updated_at=job.get("updated_at"),
                file_id=job.get("file_id") 
            )
        )

    total_pages = math.ceil(total_count / pagination.page_size) if pagination.page_size > 0 else 0

    # --- Construct Final Paginated Response --- 
    response_data = PaginatedJobResponse(
        jobs=job_responses,
        total=total_count,
        page=pagination.page,
        page_size=pagination.page_size,
        total_pages=total_pages
    )

    # Return the dictionary output of model_dump(mode='json')
    return create_success_response(data=response_data.model_dump(mode='json', exclude_none=True))

# --- Invoice Endpoints ---
@router.post("/invoices", 
              response_model=JobCreatedResponseModel, 
              status_code=status.HTTP_202_ACCEPTED, 
              summary="Submit Invoice for Parsing",
              description="Submits an invoice (previously uploaded) for parsing.")
async def submit_invoice_job(
    request_data: ParseRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
):
    """Submit an invoice for parsing."""
    user_id = auth["user_id"]
    doc_id = request_data.document_id # Use document_id from request

    # 1. Verify document exists and belongs to user
    doc_info = await verify_document_access(supabase, doc_id, user_id)
    if doc_info.get("document_type") != "invoice":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Document {doc_id} is not an invoice.")

    # 2. Create Job ID (Record creation deferred to worker)
    job_id = f"job_{uuid.uuid4()}"
    # --- REMOVED DB INSERT ---
    # job_record = { ... }
    # try:
    #     response = await supabase.table("jobs").insert(job_record).execute()
    #     ...
    # except Exception as e:
    #     ...

    # 3. Enqueue the ARQ Task
    try:
        ctx = {"arq_redis": arq_redis}
        # Pass all necessary info for the worker 
        job_params = {
            "job_id": job_id, 
            "document_id": doc_id,
            "file_id": request_data.file_id, # Pass file_id
            "user_id": user_id, # Pass user_id
            "callback_url": str(request_data.callback_url) if request_data.callback_url else None, # Pass callback_url
            "job_type": "invoice" # Set job_type to invoice
        }
        await enqueue_job(ctx, "process_document", job_params)
        print(f"Enqueued job {job_id} with params: {job_params}")
    except Exception as e:
        print(f"Fatal error: Failed to enqueue job {job_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to enqueue job: {e}")

    # 4. Return Job ID and Accepted status with timestamps
    now = datetime.now(timezone.utc)
    response_data = JobCreatedResponseModel(
        job_id=job_id, 
        status=JobStatus.PENDING,
        created_at=now, 
        updated_at=now
        )
    return create_success_response(data=response_data.model_dump(mode='json'), status_code=status.HTTP_202_ACCEPTED)

@router.get("/invoices/{job_id}", 
             response_model=JobResponseModel, 
             summary="Get Invoice Job Status",
             description="Retrieves the status and results (if available) of a specific invoice parsing job from Redis.")
async def get_invoice_job_status(
    job_id: Annotated[str, Path(description="The ID of the job to retrieve.")],
    auth: AuthData,
    # supabase: Annotated[Client, Depends(get_supabase_client)], # No longer needed
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)] # Inject Redis client
):
    """Get the status of an invoice parsing job from Redis."""
    user_id = auth["user_id"]
    # This function now fetches from Redis
    # job_data = await get_job_from_db(supabase, job_id, user_id)
    redis_key = f"job:{job_id}"

    try:
        job_json = redis_client.get(redis_key)
        if not job_json:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with ID '{job_id}' not found.")

        job_data = json.loads(job_json)
        
        # Optional: Verify ownership 
        if job_data.get("user_id") != user_id:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to this job.")
        
        # Ensure job type is correct (optional but good practice)
        if job_data.get("job_type") != "invoice":
            logger.warning(f"Job {job_id} fetched via /invoices endpoint has incorrect job_type: {job_data.get('job_type')}")
            # Depending on desired behavior, could raise 404 or proceed
            # raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with ID '{job_id}' is not an invoice job.")

        created_at_str = job_data.get("created_at")
        updated_at_str = job_data.get("updated_at")

        response_data = JobResponseModel(
            job_id=job_data.get("id", job_id),
            status=JobStatus(job_data.get("status", JobStatus.FAILED)), 
            result=job_data.get("result"),
            error=job_data.get("error_message"),
            created_at=created_at_str, 
            updated_at=updated_at_str, 
        )
        return create_success_response(data=response_data.model_dump(mode='json', exclude_none=True))

    except json.JSONDecodeError:
         logger.error(f"Job {job_id}: Failed to decode job JSON from Redis: {job_json}")
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to parse job data.")
    except redis.RedisError as e:
        logger.error(f"Job {job_id}: Redis error fetching job: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve job status.")
    except Exception as e:
        logger.error(f"Job {job_id}: Unexpected error fetching job status: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error.")

# --- NEW --- List Invoice Jobs Endpoint ---
@router.get("/invoices", 
             response_model=PaginatedJobResponse, 
             summary="List Invoice Jobs",
             description="Lists invoice parsing jobs for the authenticated user, fetching from Redis with optional status filtering and pagination.")
async def list_invoice_jobs(
    auth: AuthData,
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)],
    pagination: Annotated[PaginationParams, Depends()], 
):
    """List all invoice parsing jobs from Redis."""
    user_id = auth["user_id"]
    job_type_filter = "invoice" # Only change is this filter
    
    filtered_jobs = []
    try:
        # Iterate through all job keys in Redis
        for key in redis_client.scan_iter("job:*"):
            job_json = redis_client.get(key)
            if not job_json:
                continue
            
            try:
                job_data = json.loads(job_json)
            except json.JSONDecodeError:
                logger.warning(f"Failed to decode JSON for key {key}. Skipping.")
                continue

            # --- Apply Filters ---
            # 1. Filter by User ID (Essential Security)
            if job_data.get("user_id") != user_id:
                continue
            # 2. Filter by Job Type
            if job_data.get("job_type") != job_type_filter:
                continue
            # 3. Filter by Status (if provided)
            if pagination.status and job_data.get("status") != pagination.status.value:
                continue
            
            # Add job to list if all filters pass
            filtered_jobs.append(job_data)
            
    except redis.RedisError as e:
        logger.error(f"Redis error listing jobs for user {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to list jobs due to cache error.")

    # --- Sort by creation date (descending) ---
    filtered_jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
    
    # --- Apply Pagination ---
    total_count = len(filtered_jobs)
    start_index = (pagination.page - 1) * pagination.page_size
    end_index = start_index + pagination.page_size
    paginated_job_data = filtered_jobs[start_index:end_index]
    
    # --- Format Response Items ---
    job_responses = []
    for job in paginated_job_data:
        job_responses.append(
            JobStatusResponseModel(
                job_id=job.get("id", "unknown"),
                status=JobStatus(job.get("status", JobStatus.FAILED)),
                error_message=job.get("error_message"),
                created_at=job.get("created_at"),
                updated_at=job.get("updated_at"),
                file_id=job.get("file_id") 
            )
        )

    total_pages = math.ceil(total_count / pagination.page_size) if pagination.page_size > 0 else 0

    # --- Construct Final Paginated Response --- 
    response_data = PaginatedJobResponse(
        jobs=job_responses,
        total=total_count,
        page=pagination.page,
        page_size=pagination.page_size,
        total_pages=total_pages
    )

    # Return the dictionary output of model_dump(mode='json')
    return create_success_response(data=response_data.model_dump(mode='json', exclude_none=True))

# --- Batch Processing Endpoints --- #

# Helper to submit multiple jobs of the same type
async def _submit_batch(
    request_data: BatchRequestModel, 
    auth: AuthData, 
    supabase: Client, # Keep for verify_document_access
    arq_redis: ArqRedis, 
    redis_client: redis.Redis, # Add redis_client dependency
    job_type: str 
):
    """Handles the logic for submitting a batch job and storing metadata in Redis."""
    user_id = auth["user_id"]
    job_ids = []
    file_details_for_batch_record = [] 

    if not request_data.files:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No files provided in the batch request.")

    # 1. Iterate and enqueue individual jobs
    for file_info in request_data.files:
        doc_id = file_info.document_id or file_info.file_id # Use document_id if provided, else file_id
        file_id = file_info.file_id
        
        try:
            # Verify document access (ensure it exists and belongs to user)
            doc_info = await verify_document_access(supabase, doc_id, user_id)
            if doc_info.get("document_type") != job_type:
                 logger.warning(f"Skipping document {doc_id} in batch: Type mismatch (expected {job_type}, got {doc_info.get('document_type')})")
                 continue 

            # Generate Job ID for this specific file
            job_id = f"job_{uuid.uuid4()}"
            job_ids.append(job_id)
            file_details_for_batch_record.append({"file_id": file_id, "document_id": doc_id, "job_id": job_id})

            # Enqueue the individual job
            ctx = {"arq_redis": arq_redis}
            job_params = {
                "job_id": job_id, 
                "document_id": doc_id,
                "file_id": file_id, 
                "user_id": user_id,
                "callback_url": request_data.callback_url, # Pass batch callback?
                "job_type": job_type
            }
            await enqueue_job(ctx, "process_document", job_params)
            print(f"Enqueued batch item job {job_id} for doc {doc_id}")

        except HTTPException as e:
             logger.error(f"Error verifying document {doc_id} for batch: {e.detail}")
             raise HTTPException(status_code=e.status_code, detail=f"Error processing document {doc_id}: {e.detail}")
        except Exception as e:
            logger.error(f"Failed to enqueue job for document {doc_id} in batch: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to enqueue job for document {doc_id}")

    if not job_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid files found to process in the batch.")

    # 2. Create Batch Metadata and Store in Redis
    batch_id = f"batch_{uuid.uuid4()}"
    now = datetime.now(timezone.utc)
    batch_data = {
        "id": batch_id,
        "job_ids": job_ids,
        "status": JobStatus.PENDING.value,
        "callback_url": request_data.callback_url,
        "job_type": job_type, # Store job type
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "result": None # Initialize result field for potential future use
    }
    try:
        redis_key = f"batch:{batch_id}"
        # --- Use print for debugging --- 
        print(f"DEBUG: Preparing to store batch metadata for key {redis_key}") 
        batch_json_str = json.dumps(batch_data) # Serialize first
        print(f"DEBUG: Attempting redis_client.set with data: {batch_json_str}")
        redis_client.set(redis_key, batch_json_str, ex=24 * 3600) # 24h expiry like Go
        print(f"DEBUG: Successfully executed redis_client.set for key {redis_key}") # Changed log message
        # --- End print debugging --- 
    except json.JSONDecodeError as json_err:
        # Catch JSON serialization errors specifically
        logger.error(f"Failed to serialize batch metadata for Redis: {json_err}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to serialize batch data.")
    except Exception as e:
        # If storing batch metadata fails, individual jobs are already queued.
        # This is still problematic, but aligns with Go's potential failure point.
        logger.error(f"Failed to store batch metadata in Redis (key: {redis_key}): {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create batch tracking record in cache.")

    # 3. Return Batch Info (using data we just prepared)
    response_data = BatchCreatedResponseModel(
        batch_id=batch_id,
        status=JobStatus(batch_data["status"]), 
        total_files=len(job_ids), 
        created_at=now,
        callback_url=batch_data["callback_url"]
    )
    return create_success_response(data=response_data.model_dump(mode='json', exclude_none=True), status_code=status.HTTP_202_ACCEPTED)

@router.post("/statements/batch", 
              response_model=BatchCreatedResponseModel,
              status_code=status.HTTP_202_ACCEPTED,
              summary="Submit Batch of Bank Statements for Processing",
              description="Submits multiple bank statements (previously uploaded) for batch processing.")
async def submit_statement_batch_job(
    request_data: BatchRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)],
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)] # Inject Redis client
):
    return await _submit_batch(request_data, auth, supabase, arq_redis, redis_client, "bank_statement")

@router.post("/invoices/batch", 
              response_model=BatchCreatedResponseModel,
              status_code=status.HTTP_202_ACCEPTED,
              summary="Submit Batch of Invoices for Processing",
              description="Submits multiple invoices (previously uploaded) for batch processing.")
async def submit_invoice_batch_job(
    request_data: BatchRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)],
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)] # Inject Redis client
):
    return await _submit_batch(request_data, auth, supabase, arq_redis, redis_client, "invoice")

# --- Batch Status Endpoints ---

# Helper to get batch status (consolidates logic)
async def _get_batch_status(
    batch_id: str, 
    auth: AuthData, 
    supabase: Client, # Still needed to verify document access within loop? No, only for initial fetch if using DB.
    redis_client: redis.Redis # Added redis_client dependency
) -> BatchStatusResponseModel:
    """Retrieves batch status and individual job statuses from Redis."""
    user_id = auth["user_id"]
    
    # 1. Get Batch Metadata from Redis
    batch_redis_key = f"batch:{batch_id}"
    logger.info(f"Fetching batch metadata from Redis key: {batch_redis_key}") # Log key
    batch_json = redis_client.get(batch_redis_key)
    # --- Add debug print --- 
    print(f"DEBUG: Raw data retrieved from Redis for key {batch_redis_key}: {batch_json}")
    # --- End debug print --- 
    if not batch_json:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Batch job with ID '{batch_id}' not found.")
    
    try:
        batch_record = json.loads(batch_json)
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON for batch {batch_id}: {batch_json}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to parse batch data.")

    # --- Add Logging Here --- 
    current_user_id = user_id # User ID from auth dependency (still available, but not used for check)
    logger.info(f"Fetching batch {batch_id} for user {current_user_id}. Ownership check disabled.") # Adjust log message
    # --- End Logging --- 

    job_ids = batch_record.get("job_ids", [])
    if not job_ids:
         # Return current batch status if no job IDs are associated
         return BatchStatusResponseModel(
            batch_id=batch_id,
            status=JobStatus(batch_record.get("status", JobStatus.FAILED)),
            total_files=0,
            processed_files=0,
            failed_files=0,
            created_at=datetime.fromisoformat(batch_record["created_at"]),
            updated_at=datetime.fromisoformat(batch_record["updated_at"]),
            files=[],
            callback_url=batch_record.get("callback_url")
        )

    # 2. Get Individual Job Statuses from Redis
    file_statuses: List[BatchFileResponseModel] = []
    processed_count = 0
    failed_count = 0
    pending_or_processing_count = 0
    batch_job_data = {} # Store individual job data if needed
    overall_status = JobStatus.PROCESSING # Assume processing until proven otherwise by checking jobs
    allCompleted = True
    allFailed = True

    try:
        # Use Redis pipeline for efficiency
        pipe = redis_client.pipeline()
        for job_id in job_ids:
            pipe.get(f"job:{job_id}")
        job_jsons = pipe.execute()
    except redis.RedisError as e:
        logger.error(f"Batch {batch_id}: Redis error fetching job statuses: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve job statuses.")

    for i, job_json in enumerate(job_jsons):
        job_id = job_ids[i]
        job_status = JobStatus.FAILED # Renamed from status
        error_msg = "Job data not found in cache (expired or failed before processing)." 
        file_id_for_response = None 
        doc_id_for_response = None 
        
        if job_json:
            try:
                job_data = json.loads(job_json)
                batch_job_data[job_id] = job_data 
                job_status = JobStatus(job_data.get("status", JobStatus.FAILED)) # Renamed from status
                error_msg = job_data.get("error_message")
                file_id_for_response = job_data.get("file_id")
                doc_id_for_response = job_data.get("document_id")
            except json.JSONDecodeError:
                 logger.error(f"Batch {batch_id}: Failed to decode JSON for job {job_id}: {job_json}")
                 job_status = JobStatus.FAILED # Renamed from status
                 error_msg = "Failed to parse job data from cache."
        
        # Append status for this file
        file_statuses.append(BatchFileResponseModel(
            file_id=file_id_for_response or "unknown", 
            document_id=doc_id_for_response,
            status=job_status, # Use renamed variable
            error=error_msg if job_status == JobStatus.FAILED else None # Use renamed variable
        ))

        # Count statuses AND update overall batch status flags 
        if job_status == JobStatus.COMPLETED: # Use renamed variable
            processed_count += 1
            allFailed = False 
        elif job_status == JobStatus.FAILED: # Use renamed variable
            failed_count += 1
            allCompleted = False 
        elif job_status == JobStatus.PENDING or job_status == JobStatus.PROCESSING: # Use renamed variable
            pending_or_processing_count += 1
            allCompleted = False
            allFailed = False
            overall_status = JobStatus.PROCESSING
            # break # Can break early as batch is still processing

    # 3. Determine Final Overall Batch Status 
    if allCompleted:
        overall_status = JobStatus.COMPLETED
    elif allFailed:
        overall_status = JobStatus.FAILED
    else:
        overall_status = JobStatus.FAILED 
        logger.warning(f"Batch {batch_id}: Mixed final job states (completed/failed), marking batch as FAILED.")

    # 4. Construct Final Response
    response_data = BatchStatusResponseModel(
        batch_id=batch_id,
        status=overall_status, 
        total_files=len(job_ids),
        processed_files=processed_count,
        failed_files=failed_count,
        created_at=datetime.fromisoformat(batch_record["created_at"]),
        updated_at=datetime.fromisoformat(batch_record["updated_at"]), # Use timestamp from batch record
        files=file_statuses,
        callback_url=batch_record.get("callback_url")
    )
    return response_data 

@router.get("/statements/batch/{batch_id}", 
             response_model=BatchStatusResponseModel,
             summary="Get Bank Statement Batch Status",
             description="Retrieves the status and results of a batch processing job for bank statements.")
async def get_statement_batch_status(
    batch_id: Annotated[str, Path(description="The ID of the batch to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)], # Keep Supabase if needed elsewhere? No.
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)] # Inject Redis
):
    # Pass supabase=None if not needed, or remove entirely if _get_batch_status doesn't need it.
    batch_status_data = await _get_batch_status(batch_id, auth, None, redis_client)
    return create_success_response(data=batch_status_data.model_dump(mode='json', exclude_none=True))

@router.get("/invoices/batch/{batch_id}", 
             response_model=BatchStatusResponseModel,
             summary="Get Invoice Batch Status",
             description="Retrieves the status and results of a batch processing job for invoices.")
async def get_invoice_batch_status(
    batch_id: Annotated[str, Path(description="The ID of the batch to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)], # Keep Supabase if needed elsewhere? No.
    redis_client: Annotated[redis.Redis, Depends(get_sync_redis_client_for_api)] # Inject Redis
):
    # Pass supabase=None if not needed, or remove entirely if _get_batch_status doesn't need it.
    batch_status_data = await _get_batch_status(batch_id, auth, None, redis_client)
    return create_success_response(data=batch_status_data.model_dump(mode='json', exclude_none=True))

# Note: Also commenting out the /parse endpoint for now to fully isolate /statements
# --- General Parse Endpoint ---
# @router.post("", 
#               response_model=JobCreatedResponseModel, 
#               status_code=status.HTTP_202_ACCEPTED, 
#               summary="Submit Document for Parsing",
#               description="General endpoint that routes to the appropriate handler based on document type.")
# async def general_parse(
#     request_data: ParseRequestModel,
#     auth: AuthData,
#     supabase: Annotated[Client, Depends(get_supabase_client)],
#     arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
# ):
#     # ... (function body remains unchanged) ... 