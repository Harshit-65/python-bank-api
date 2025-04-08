from typing import List, Optional, Annotated
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from supabase import Client
import uuid
import math
from datetime import datetime, timezone
from arq.connections import ArqRedis
import json

from ...db.supabase_client import get_supabase_client
from ...auth.dependencies import AuthData
from ...queue.arq_client import get_arq_redis, enqueue_job
from ..models.requests import ParseRequestModel, PaginationParams, JobStatus, BatchRequestModel
from ..models.responses import (
    JobCreatedResponseModel, JobStatusResponseModel, 
    PaginatedJobResponse, BatchCreatedResponseModel, 
    BatchStatusResponseModel, JobResponseModel # Added JobResponseModel
)
from ..utils.response_utils import create_success_response, create_error_response, APIErrorModel # Import error utils

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
async def create_batch_job_record(supabase: Client, job_ids: List[str], callback_url: Optional[str], user_id: str, job_type: str) -> str:
    """Creates a batch job record in the database."""
    batch_id = f"batch_{uuid.uuid4()}"
    batch_record = {
        "id": batch_id,
        "job_ids": job_ids,
        "status": JobStatus.PENDING.value,
        "callback_url": callback_url, # Store directly (already str or None)
        "user_id": user_id,
        "job_type": job_type, # Track batch type
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        response = await supabase.table("batch_jobs").insert(batch_record).execute()
        # Simple error check (adjust based on Supabase client specifics)
        if hasattr(response, 'error') and response.error:
             raise Exception(f"Supabase error: {response.error}")
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
    tags=["Parsing Jobs"],
    dependencies=[Depends(AuthData)],
)

# --- Helper to Verify Document --- 
async def verify_document_access(supabase: Client, doc_id: str, user_id: str):
    """Verify the user owns the document and it exists."""
    try:
        doc_response = await supabase.table("documents").select("id, document_type").eq("id", doc_id).eq("user_id", user_id).maybe_single().execute()
        if not doc_response.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Document with ID '{doc_id}' not found or access denied.")
        return doc_response.data # Return doc info (including type)
    except HTTPException: # Re-raise HTTPException
        raise
    except Exception as e:
        print(f"Error verifying document {doc_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error verifying document")

# --- General Parse Endpoint ---
@router.post("", 
              response_model=JobCreatedResponseModel, 
              status_code=status.HTTP_202_ACCEPTED, 
              summary="Submit Document for Parsing",
              description="General endpoint that routes to the appropriate handler based on document type.")
async def general_parse(
    request_data: ParseRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
):
    """General parse endpoint that routes to the appropriate handler based on document type."""
    user_id = auth["user_id"]
    doc_id = request_data.file_id # Use file_id from request as document_id
    
    # Get document details to determine document type
    try:
        doc_response = await supabase.table("documents").select("document_type").eq("id", doc_id).eq("user_id", user_id).maybe_single().execute()
        if not doc_response.data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Document with ID '{doc_id}' not found or access denied.")
        
        doc_type = doc_response.data.get("document_type")
        if doc_type == "bank_statement":
            # Route to bank statement handler
            return await submit_statement_job(request_data, auth, supabase, arq_redis)
        elif doc_type == "invoice":
            # Route to invoice handler
            return await submit_invoice_job(request_data, auth, supabase, arq_redis)
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported document type: {doc_type}")
    except Exception as e:
        print(f"Error routing document {doc_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error routing document: {e}")

# --- Statement Endpoints ---
@router.post("/statements", 
              response_model=JobCreatedResponseModel, 
              status_code=status.HTTP_202_ACCEPTED, 
              summary="Submit Bank Statement for Parsing",
              description="Submits a bank statement (previously uploaded) for parsing.")
async def submit_statement_job(
    request_data: ParseRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
):
    """Submit a bank statement for parsing."""
    user_id = auth["user_id"]
    doc_id = request_data.document_id # Use document_id from request

    # 1. Verify document exists and belongs to user
    doc_info = await verify_document_access(supabase, doc_id, user_id)
    if doc_info.get("document_type") != "bank_statement":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Document {doc_id} is not a bank statement.")

    # 2. Create Job Record
    job_id = f"job_{uuid.uuid4()}"
    job_record = {
        "id": job_id,
        "document_id": doc_id,
        "status": JobStatus.PENDING.value,
        "callback_url": str(request_data.callback_url) if request_data.callback_url else None,
        "job_type": "bank_statement", 
        "user_id": user_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        response = await supabase.table("jobs").insert(job_record).execute()
        if hasattr(response, 'error') and response.error:
             raise Exception(f"Supabase error: {response.error}")
    except Exception as e:
        print(f"Error creating job record for doc {doc_id}: {e}")
        # Consider rolling back or cleaning up if needed
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create job record")

    # 3. Enqueue the ARQ Task
    try:
        ctx = {"arq_redis": arq_redis}
        job_params = {"job_id": job_id, "document_id": doc_id}
        await enqueue_job(ctx, "process_document", job_params)
    except Exception as e:
        # If enqueue fails, update DB job status to FAILED
        print(f"Error enqueuing job {job_id}: {e}. Updating job status to failed.")
        try:
            await supabase.table("jobs").update({"status": JobStatus.FAILED.value, "error_message": "Enqueue failed"}).eq("id", job_id).execute()
        except Exception as db_err:
            print(f"Error updating job {job_id} status to failed after enqueue error: {db_err}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to enqueue job: {e}")

    # 4. Return Job ID and Accepted status
    response_data = JobCreatedResponseModel(job_id=job_id, status=JobStatus.PENDING)
    return create_success_response(data=response_data.model_dump(), status_code=status.HTTP_202_ACCEPTED)

@router.get("/statements/{job_id}", 
             # Use JobResponseModel which includes more details like file_id via result
             response_model=JobResponseModel, 
             summary="Get Bank Statement Job Status",
             description="Retrieves the status and results (if available) of a specific bank statement parsing job.")
async def get_statement_job_status(
    job_id: Annotated[str, Path(description="The ID of the job to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
):
    """Get the status of a bank statement parsing job."""
    user_id = auth["user_id"]
    job_data = await get_job_from_db(supabase, job_id, user_id)

    if not job_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with ID '{job_id}' not found or access denied.")
    
    # Parse results if they exist
    results_dict = None
    if job_data.get("results"): 
        try:
            results_dict = json.loads(job_data["results"])
        except json.JSONDecodeError:
            results_dict = {"error": "Failed to parse results JSON"}

    response_data = JobResponseModel(
        job_id=job_data["id"],
        status=JobStatus(job_data["status"]), 
        result=results_dict, 
        error=job_data.get("error_message"),
        created_at=datetime.fromisoformat(job_data["created_at"]), # Ensure datetime object
        updated_at=datetime.fromisoformat(job_data["updated_at"]), # Ensure datetime object
    )
    return create_success_response(data=response_data.model_dump(exclude_none=True))

@router.get("/statements", 
             response_model=PaginatedJobResponse, 
             summary="List Bank Statement Jobs",
             description="Lists bank statement parsing jobs for the authenticated user, with optional status filtering and pagination.")
async def list_statement_jobs(
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    pagination: Annotated[PaginationParams, Depends()], 
):
    """List all bank statement parsing jobs."""
    user_id = auth["user_id"]
    
    jobs_data, total_count = await list_jobs_from_db(supabase, user_id, "bank_statement", pagination)
    
    job_responses = []
    for job in jobs_data:
        results_dict = None
        if job.get("results"): 
            try:
                results_dict = json.loads(job["results"])
            except json.JSONDecodeError:
                results_dict = {"error": "Failed to parse results JSON"}
        
        job_responses.append(
            JobStatusResponseModel( # Use JobStatusResponseModel for lists
                job_id=job["id"],
                status=JobStatus(job["status"]), 
                result=results_dict, 
                error_message=job.get("error_message"),
                created_at=datetime.fromisoformat(job["created_at"]), 
                updated_at=datetime.fromisoformat(job["updated_at"]),
                file_id=job.get("documents", {}).get("file_name") # Get filename if joined
            )
        )

    total_pages = math.ceil(total_count / pagination.page_size) if pagination.page_size > 0 else 0

    response_data = PaginatedJobResponse(
        jobs=job_responses,
        total=total_count,
        page=pagination.page,
        page_size=pagination.page_size,
        total_pages=total_pages
    )

    return create_success_response(data=response_data.model_dump(exclude_none=True))

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

    # 2. Create Job Record
    job_id = f"job_{uuid.uuid4()}"
    job_record = {
        "id": job_id,
        "document_id": doc_id,
        "status": JobStatus.PENDING.value,
        "callback_url": str(request_data.callback_url) if request_data.callback_url else None,
        "job_type": "invoice", 
        "user_id": user_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        response = await supabase.table("jobs").insert(job_record).execute()
        if hasattr(response, 'error') and response.error:
             raise Exception(f"Supabase error: {response.error}")
    except Exception as e:
        print(f"Error creating job record for doc {doc_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create job record")

    # 3. Enqueue the ARQ Task
    try:
        ctx = {"arq_redis": arq_redis}
        job_params = {"job_id": job_id, "document_id": doc_id}
        await enqueue_job(ctx, "process_document", job_params)
    except Exception as e:
        print(f"Error enqueuing job {job_id}: {e}. Updating job status to failed.")
        try:
            await supabase.table("jobs").update({"status": JobStatus.FAILED.value, "error_message": "Enqueue failed"}).eq("id", job_id).execute()
        except Exception as db_err:
            print(f"Error updating job {job_id} status to failed after enqueue error: {db_err}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to enqueue job: {e}")

    # 4. Return Job ID and Accepted status
    response_data = JobCreatedResponseModel(job_id=job_id, status=JobStatus.PENDING)
    return create_success_response(data=response_data.model_dump(), status_code=status.HTTP_202_ACCEPTED)

@router.get("/invoices/{job_id}", 
             response_model=JobResponseModel, # Use JobResponseModel 
             summary="Get Invoice Job Status",
             description="Retrieves the status and results (if available) of a specific invoice parsing job.")
async def get_invoice_job_status(
    job_id: Annotated[str, Path(description="The ID of the job to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
):
    """Get the status of an invoice parsing job."""
    user_id = auth["user_id"]
    job_data = await get_job_from_db(supabase, job_id, user_id)

    if not job_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with ID '{job_id}' not found or access denied.")
    
    results_dict = None
    if job_data.get("results"): 
        try:
            results_dict = json.loads(job_data["results"])
        except json.JSONDecodeError:
            results_dict = {"error": "Failed to parse results JSON"}

    response_data = JobResponseModel(
        job_id=job_data["id"],
        status=JobStatus(job_data["status"]), 
        result=results_dict, 
        error=job_data.get("error_message"),
        created_at=datetime.fromisoformat(job_data["created_at"]), 
        updated_at=datetime.fromisoformat(job_data["updated_at"]), 
    )
    return create_success_response(data=response_data.model_dump(exclude_none=True))

@router.get("/invoices", 
             response_model=PaginatedJobResponse, 
             summary="List Invoice Jobs",
             description="Lists invoice parsing jobs for the authenticated user, with optional status filtering and pagination.")
async def list_invoice_jobs(
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    pagination: Annotated[PaginationParams, Depends()], 
):
    """List all invoice parsing jobs."""
    user_id = auth["user_id"]
    
    jobs_data, total_count = await list_jobs_from_db(supabase, user_id, "invoice", pagination)
    
    job_responses = []
    for job in jobs_data:
        results_dict = None
        if job.get("results"): 
            try:
                results_dict = json.loads(job["results"])
            except json.JSONDecodeError:
                results_dict = {"error": "Failed to parse results JSON"}
        
        job_responses.append(
            JobStatusResponseModel( # Use JobStatusResponseModel for lists
                job_id=job["id"],
                status=JobStatus(job["status"]), 
                result=results_dict, 
                error_message=job.get("error_message"),
                created_at=datetime.fromisoformat(job["created_at"]), 
                updated_at=datetime.fromisoformat(job["updated_at"]), 
                file_id=job.get("documents", {}).get("file_name") 
            )
        )

    total_pages = math.ceil(total_count / pagination.page_size) if pagination.page_size > 0 else 0

    response_data = PaginatedJobResponse(
        jobs=job_responses,
        total=total_count,
        page=pagination.page,
        page_size=pagination.page_size,
        total_pages=total_pages
    )

    return create_success_response(data=response_data.model_dump(exclude_none=True))

# --- Batch Processing Endpoints --- #

# Helper to submit multiple jobs of the same type
async def _submit_batch(request_data: BatchRequestModel, auth: AuthData, supabase: Client, arq_redis: ArqRedis, job_type: str):
    user_id = auth["user_id"]
    created_job_ids = []
    ctx = {"arq_redis": arq_redis}
    processed_doc_ids = set() # Prevent duplicate processing in same batch
    
    for file_model in request_data.files:
        doc_id = file_model.document_id # Use document_id from request
        if not doc_id or doc_id in processed_doc_ids:
            print(f"Skipping document_id: {doc_id} (missing or already processed in batch)")
            continue

        try:
            # Verify document access AND type
            doc_info = await verify_document_access(supabase, doc_id, user_id)
            if doc_info.get("document_type") != job_type:
                print(f"Skipping document {doc_id}: incorrect type ({doc_info.get("document_type")}) for {job_type} batch.")
                continue # Skip if type doesn't match batch type

            # Create Job Record
            job_id = f"job_{uuid.uuid4()}"
            job_record = {
                "id": job_id,
                "document_id": doc_id,
                "status": JobStatus.PENDING.value,
                "callback_url": None, # Individual job callbacks not used for batch
                "job_type": job_type,
                "user_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            await supabase.table("jobs").insert(job_record).execute()

            # Enqueue Task
            job_params = {"job_id": job_id, "document_id": doc_id}
            await enqueue_job(ctx, "process_document", job_params)
            created_job_ids.append(job_id)
            processed_doc_ids.add(doc_id)

        except HTTPException as e:
            # Log HTTP exceptions (like 404 Not Found) but continue
            print(f"Skipping document {doc_id} due to error: {e.detail}")
        except Exception as e:
            # Log other errors and continue
            print(f"Error processing document {doc_id} in batch: {e}")
            # Optionally, create a job record with status FAILED here

    if not created_job_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid documents found or jobs created in the batch request.")

    # Create Batch Metadata Record
    batch_id = await create_batch_job_record(supabase, created_job_ids, request_data.callback_url, user_id, job_type)

    # Return Batch Info
    response_data = BatchCreatedResponseModel(
        batch_id=batch_id,
        status=JobStatus.PENDING,
        total_files=len(created_job_ids),
        created_at=datetime.now(timezone.utc),
        callback_url=request_data.callback_url
    )
    return create_success_response(data=response_data.model_dump(exclude_none=True), status_code=status.HTTP_202_ACCEPTED)

@router.post("/statements/batch", 
              response_model=BatchCreatedResponseModel,
              status_code=status.HTTP_202_ACCEPTED,
              summary="Submit Batch of Bank Statements for Processing",
              description="Submits multiple bank statements (previously uploaded) for batch processing.")
async def submit_statement_batch_job(
    request_data: BatchRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
):
    return await _submit_batch(request_data, auth, supabase, arq_redis, "bank_statement")

@router.post("/invoices/batch", 
              response_model=BatchCreatedResponseModel,
              status_code=status.HTTP_202_ACCEPTED,
              summary="Submit Batch of Invoices for Processing",
              description="Submits multiple invoices (previously uploaded) for batch processing.")
async def submit_invoice_batch_job(
    request_data: BatchRequestModel,
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
    arq_redis: Annotated[ArqRedis, Depends(get_arq_redis)]
):
    return await _submit_batch(request_data, auth, supabase, arq_redis, "invoice")

# --- Batch Status Endpoints ---

# Helper to get batch status (consolidates logic)
async def _get_batch_status(batch_id: str, auth: AuthData, supabase: Client, job_type: str) -> BatchStatusResponseModel:
    user_id = auth["user_id"]
    batch_record = await get_batch_job_record(supabase, batch_id, user_id)

    if not batch_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Batch with ID '{batch_id}' not found or access denied")
    
    # Verify batch type matches endpoint
    if batch_record.get("job_type") != job_type:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Batch '{batch_id}' is not a {job_type} batch.")

    job_ids_in_batch = batch_record.get("job_ids", [])
    total_files = len(job_ids_in_batch)
    processed_count = 0
    failed_count = 0
    current_batch_status = JobStatus(batch_record["status"]) # Start with DB status
    all_files_info = []
    
    if not job_ids_in_batch:
        # Handle empty batch case
         return BatchStatusResponseModel(
            batch_id=batch_record["id"],
            status=current_batch_status, # Should likely be PENDING or maybe COMPLETED if 0 files?
            total_files=0,
            processed_files=0,
            failed_files=0,
            created_at=datetime.fromisoformat(batch_record["created_at"]),
            updated_at=datetime.fromisoformat(batch_record["updated_at"]),
            files=[],
            callback_url=batch_record.get("callback_url")
        )

    # Fetch status of individual jobs
    try:
        query = supabase.table("jobs").select("id, status, error_message, document_id, documents(file_name)").in_("id", job_ids_in_batch).eq("user_id", user_id)
        jobs_response = await query.execute()
        jobs_data = jobs_response.data or []
    except Exception as e:
        print(f"Error fetching job statuses for batch {batch_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch job statuses")

    job_statuses_map = {job['id']: job for job in jobs_data}

    all_terminal = True # Assume all jobs are completed or failed
    any_processing = False
    any_pending = False

    for job_id in job_ids_in_batch:
        job_info = job_statuses_map.get(job_id)
        file_status_info = {
            "file_id": "Unknown",
            "document_id": None,
            "status": JobStatus.FAILED,
            "error": "Job record not found or inaccessible"
        }
        current_status = JobStatus.FAILED

        if job_info:
            current_status = JobStatus(job_info["status"]) 
            file_status_info = {
                "file_id": job_info.get("documents", {}).get("file_name", "N/A"),
                "document_id": job_info["document_id"],
                "status": current_status,
                "error": job_info.get("error_message")
            }
        else:
             # If job not found for user, count as failed towards batch status
             failed_count += 1

        all_files_info.append(file_status_info)

        # Update counts and flags based on actual job status
        if current_status == JobStatus.COMPLETED:
            processed_count += 1
        elif current_status == JobStatus.FAILED:
            processed_count += 1 # Count failed as processed for progress
            failed_count += 1
            # All terminal flag remains true
        elif current_status == JobStatus.PROCESSING:
            any_processing = True
            all_terminal = False
        else: # PENDING
            any_pending = True
            all_terminal = False

    # Determine overall batch status based on individual jobs
    new_batch_status = current_batch_status
    if all_terminal:
        if failed_count > 0:
            new_batch_status = JobStatus.FAILED # Or a PARTIAL_FAILURE status?
        else:
            new_batch_status = JobStatus.COMPLETED
    elif any_processing:
        new_batch_status = JobStatus.PROCESSING
    elif any_pending: # Only pending jobs left
        new_batch_status = JobStatus.PENDING
    # Add more nuanced logic if needed (e.g., PARTIAL_FAILURE)

    # Update the batch record in DB if status has changed
    if new_batch_status != current_batch_status:
        try:
            await supabase.table("batch_jobs").update({"status": new_batch_status.value, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", batch_id).execute()
            current_batch_status = new_batch_status # Reflect the update
        except Exception as e:
             print(f"Warning: Failed to update batch {batch_id} status in DB: {e}")
             # Use the calculated status for the response even if DB update fails
             current_batch_status = new_batch_status 

    response_data = BatchStatusResponseModel(
        batch_id=batch_record["id"],
        status=current_batch_status,
        total_files=total_files,
        processed_files=processed_count, # Reflects completed + failed
        failed_files=failed_count,
        created_at=datetime.fromisoformat(batch_record["created_at"]),
        updated_at=datetime.fromisoformat(batch_record["updated_at"]),
        files=all_files_info,
        callback_url=batch_record.get("callback_url")
    )
    
    return response_data # Return the Pydantic model directly

@router.get("/statements/batch/{batch_id}", 
             response_model=BatchStatusResponseModel,
             summary="Get Bank Statement Batch Status",
             description="Retrieves the status and results of a batch processing job for bank statements.")
async def get_statement_batch_status(
    batch_id: Annotated[str, Path(description="The ID of the batch to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
):
    batch_status_data = await _get_batch_status(batch_id, auth, supabase, "bank_statement")
    return create_success_response(data=batch_status_data.model_dump(exclude_none=True))

@router.get("/invoices/batch/{batch_id}", 
             response_model=BatchStatusResponseModel,
             summary="Get Invoice Batch Status",
             description="Retrieves the status and results of a batch processing job for invoices.")
async def get_invoice_batch_status(
    batch_id: Annotated[str, Path(description="The ID of the batch to retrieve.")],
    auth: AuthData,
    supabase: Annotated[Client, Depends(get_supabase_client)],
):
    batch_status_data = await _get_batch_status(batch_id, auth, supabase, "invoice")
    return create_success_response(data=batch_status_data.model_dump(exclude_none=True)) 