from typing import List, Optional, Annotated
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from supabase import Client
import uuid
import math
from datetime import datetime
from arq.connections import ArqRedis
import json

from ...db.supabase_client import get_supabase_client
from ...auth.dependencies import AuthData
from ...queue.arq_client import get_arq_redis, enqueue_job
from ..models.requests import ParseRequestModel, PaginationParams, JobStatus, BatchRequestModel
from ..models.responses import (JobCreatedResponseModel, JobStatusResponseModel, 
                                PaginatedJobResponse, BatchCreatedResponseModel, 
                                BatchStatusResponseModel)
from ..utils.response_utils import create_success_response

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
async def create_batch_job_record(supabase: Client, job_ids: List[str], callback_url: Optional[str], user_id: str) -> str:
    """Creates a batch job record in the database."""
    batch_id = f"batch_{uuid.uuid4()}"
    batch_record = {
        "id": batch_id,
        "job_ids": job_ids,
        "status": JobStatus.PENDING.value,
        "callback_url": str(callback_url) if callback_url else None,
        "user_id": user_id,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    try:
        await supabase.table("batch_jobs").insert(batch_record).execute()
        return batch_id
    except Exception as e:
        print(f"Error creating batch job record: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create batch job record")

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
    prefix="/parse",
    tags=["Parsing Jobs"],
    dependencies=[Depends(AuthData)],
)

# Endpoint to trigger parsing (Statements or Invoices)
@router.post("/{job_type}", 
              response_model=JobCreatedResponseModel, 
              status_code=status.HTTP_202_ACCEPTED, 
              summary="Submit Document for Parsing",
              description="Submits an uploaded document for bank statement or invoice parsing.")
async def submit_parsing_job(
    job_type: Annotated[str, Path(description="Type of document to parse ('statements' or 'invoices')", pattern="^(statements|invoices)$")],
    request_data: ParseRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
):
    user_id = auth["user_id"]
    doc_id = request_data.file_id # Use file_id from request as document_id
    actual_job_type = 'bank_statement' if job_type == 'statements' else 'invoice'

    # 1. Verify document exists and belongs to user
    try:
        doc_response = await supabase.table("documents").select("id").eq("id", doc_id).eq("user_id", user_id).maybe_single().execute()
        if not doc_response.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Document with ID '{doc_id}' not found or access denied.")
    except Exception as e:
        print(f"Error verifying document {doc_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error verifying document")

    # 2. Create Job Record FIRST (to get job_id)
    job_id = f"job_{uuid.uuid4()}"
    job_record = {
        "id": job_id,
        "document_id": doc_id,
        "status": JobStatus.PENDING.value,
        "callback_url": str(request_data.callback_url) if request_data.callback_url else None,
        "job_type": actual_job_type, 
        "user_id": user_id,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    try:
        await supabase.table("jobs").insert(job_record).execute()
    except Exception as e:
        print(f"Error creating job record for doc {doc_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create job record")

    # 3. Enqueue the ARQ Task
    try:
        # Pass ARQ client via context dictionary
        ctx = {"arq_redis": arq_redis}
        await enqueue_job(ctx, "process_document", job_id, doc_id)
    except Exception as e:
        # If enqueue fails, potentially update DB job status to FAILED
        # await supabase.table("jobs").update({"status": JobStatus.FAILED.value, "error_message": "Enqueue failed"}).eq("id", job_id).execute()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to enqueue job: {e}")

    # 4. Return Job ID and Accepted status
    response_data = JobCreatedResponseModel(job_id=job_id)
    return create_success_response(data=response_data.model_dump(), status_code=status.HTTP_202_ACCEPTED)


# Endpoint to get job status
@router.get("/{job_type}/{job_id}", 
             response_model=JobStatusResponseModel, 
             summary="Get Job Status",
             description="Retrieves the status and results (if available) of a specific parsing job.")
async def get_job_status(
    job_type: Annotated[str, Path(description="Type of job ('statements' or 'invoices')", pattern="^(statements|invoices)$")],
    job_id: Annotated[str, Path(description="The ID of the job to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
):
    user_id = auth["user_id"]
    job_data = await get_job_from_db(supabase, job_id, user_id)

    if not job_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with ID '{job_id}' not found or access denied.")
    
    response_data = JobStatusResponseModel(
        job_id=job_data["id"],
        status=JobStatus(job_data["status"]), 
        results=json.loads(job_data.get("results")) if job_data.get("results") else None, # Parse results from JSON string
        created_at=job_data["created_at"],
        updated_at=job_data["updated_at"],
        file_id=job_data.get("documents", {}).get("file_name") 
    )
    return create_success_response(data=response_data.model_dump(exclude_none=True))

# Endpoint to list jobs
@router.get("/{job_type}", 
             response_model=PaginatedJobResponse, 
             summary="List Parsing Jobs",
             description="Lists parsing jobs for the authenticated user, with optional status filtering and pagination.")
async def list_jobs(
    job_type: Annotated[str, Path(description="Type of jobs to list ('statements' or 'invoices')", pattern="^(statements|invoices)$")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    pagination: Annotated[PaginationParams, Depends()], 
):
    user_id = auth["user_id"]
    actual_job_type = 'bank_statement' if job_type == 'statements' else 'invoice'
    
    jobs_data, total_count = await list_jobs_from_db(supabase, user_id, actual_job_type, pagination)
    
    job_responses = [
        JobStatusResponseModel(
            job_id=job["id"],
            status=JobStatus(job["status"]), 
            results=json.loads(job.get("results")) if job.get("results") else None, 
            created_at=job["created_at"],
            updated_at=job["updated_at"],
            file_id=job.get("documents", {}).get("file_name")
        ) for job in jobs_data
    ]

    total_pages = math.ceil(total_count / pagination.page_size) if pagination.page_size > 0 else 0

    response_data = PaginatedJobResponse(
        jobs=job_responses,
        total=total_count,
        page=pagination.page,
        page_size=pagination.page_size,
        total_pages=total_pages
    )

    return create_success_response(data=response_data.model_dump(exclude_none=True))

# --- Batch Processing Endpoints ---

@router.post("/{job_type}/batch", 
              response_model=BatchCreatedResponseModel,
              status_code=status.HTTP_202_ACCEPTED,
              summary="Submit Batch for Processing",
              description="Submits multiple documents for batch processing.")
async def submit_batch_job(
    job_type: Annotated[str, Path(description="Type of documents to parse ('statements' or 'invoices')", pattern="^(statements|invoices)$")],
    request_data: BatchRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
):
    user_id = auth["user_id"]
    created_job_ids = []
    actual_job_type = 'bank_statement' if job_type == 'statements' else 'invoice'
    ctx = {"arq_redis": arq_redis} # Context for enqueue_job
    
    # Process each file in the batch
    for file_model in request_data.files:
        doc_id = file_model.file_id # Use file_id from batch request
        # Verify document exists and belongs to user
        try:
            doc_response = await supabase.table("documents").select("id").eq("id", doc_id).eq("user_id", user_id).maybe_single().execute()
            if not doc_response.data:
                print(f"Document with ID '{doc_id}' not found or access denied. Skipping.")
                continue
                
            # Create job record first
            job_id = f"job_{uuid.uuid4()}"
            job_record = {
                "id": job_id,
                "document_id": doc_id,
                "status": JobStatus.PENDING.value,
                "callback_url": None, # No individual callbacks for batch items
                "job_type": actual_job_type, 
                "user_id": user_id,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            }
            await supabase.table("jobs").insert(job_record).execute()
            
            # Enqueue the task
            await enqueue_job(ctx, "process_document", job_id, doc_id)
            created_job_ids.append(job_id)
            
        except Exception as e:
            print(f"Error processing file {doc_id} in batch: {e}")
            # Continue with other files even if one fails
    
    if not created_job_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid documents found or jobs created in batch")
    
    # Create batch job record (metadata)
    batch_id = await create_batch_job_record(supabase, created_job_ids, request_data.callback_url, user_id)
    
    # Return batch information
    response_data = BatchCreatedResponseModel(
        batch_id=batch_id,
        status=JobStatus.PENDING, 
        total_files=len(created_job_ids), # Use count of successfully created jobs
        created_at=datetime.now(), # Reflects batch creation time
        callback_url=str(request_data.callback_url) if request_data.callback_url else None
    )
    
    return create_success_response(data=response_data.model_dump(exclude_none=True), status_code=status.HTTP_202_ACCEPTED)

# Endpoint to get batch status
@router.get("/{job_type}/batch/{batch_id}", 
             response_model=BatchStatusResponseModel,
             summary="Get Batch Status",
             description="Retrieves the status and results of a batch processing job.")
async def get_batch_status(
    job_type: Annotated[str, Path(description="Type of batch ('statements' or 'invoices')", pattern="^(statements|invoices)$")],
    batch_id: Annotated[str, Path(description="The ID of the batch to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
):
    user_id = auth["user_id"]
    batch_record = await get_batch_job_record(supabase, batch_id, user_id)
    
    if not batch_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Batch with ID '{batch_id}' not found or access denied")

    job_ids_in_batch = batch_record.get("job_ids", [])
    total_files = len(job_ids_in_batch)
    processed_files = 0
    failed_files = 0
    batch_status = JobStatus(batch_record["status"]) # Current overall status
    all_files_info = [] 

    # Check status of each job in the batch
    if job_ids_in_batch:
        query = supabase.table("jobs").select("id, status, error_message, document_id, documents(file_name)").in_("id", job_ids_in_batch).eq("user_id", user_id)
        jobs_response = await query.execute()
        jobs_data = jobs_response.data or []
        
        job_statuses = {job['id']: job for job in jobs_data}
        
        all_completed = True
        any_failed = False
        any_processing = False

        for job_id in job_ids_in_batch:
            job_info = job_statuses.get(job_id)
            if job_info:
                current_status = JobStatus(job_info["status"])
                file_info = {
                    "file_id": job_info.get("documents", {}).get("file_name", "N/A"),
                    "document_id": job_info["document_id"],
                    "status": current_status,
                    "error": job_info.get("error_message")
                }
                all_files_info.append(file_info)

                if current_status == JobStatus.COMPLETED:
                    processed_files += 1
                elif current_status == JobStatus.FAILED:
                    failed_files += 1
                    processed_files += 1 # Count failed as processed for progress
                    any_failed = True
                    all_completed = False
                elif current_status == JobStatus.PROCESSING:
                     any_processing = True
                     all_completed = False
                else: # PENDING
                    all_completed = False
            else:
                 # Job record missing? Should ideally not happen if created.
                 all_files_info.append({"file_id": "Unknown", "document_id": None, "status": JobStatus.FAILED, "error": "Job record not found"})
                 failed_files += 1
                 processed_files += 1
                 any_failed = True
                 all_completed = False

        # Update overall batch status based on individual jobs
        if all_completed:
             batch_status = JobStatus.COMPLETED
        elif any_failed and processed_files == total_files:
             batch_status = JobStatus.FAILED # Or a new 'partial_failure' status?
        elif any_processing or processed_files < total_files:
             batch_status = JobStatus.PROCESSING
        # else: remains PENDING if nothing started
        
        # Update the batch record in DB if status changed (optional, prevents re-calculation)
        if batch_status.value != batch_record["status"]:
            try:
                await supabase.table("batch_jobs").update({"status": batch_status.value, "updated_at": datetime.now().isoformat()}).eq("id", batch_id).execute()
            except Exception as e:
                 print(f"Warning: Failed to update batch {batch_id} status in DB: {e}")

    response_data = BatchStatusResponseModel(
        batch_id=batch_record["id"],
        status=batch_status,
        total_files=total_files,
        processed_files=processed_files,
        failed_files=failed_files,
        created_at=batch_record["created_at"],
        updated_at=batch_record["updated_at"],
        files=all_files_info,
        callback_url=batch_record.get("callback_url")
    )
    
    return create_success_response(data=response_data.model_dump(exclude_none=True)) 