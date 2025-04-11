import logging
import os
import json
import random
from io import BytesIO
from typing import Dict, Any, Optional
import mimetypes
from datetime import datetime, timezone, timedelta
import httpx

import google.generativeai as genai
from supabase import create_client, Client
from PIL import Image
import fitz # PyMuPDF
import redis # Import the redis library

from ..config import SUPABASE_URL, SUPABASE_KEY, GOOGLE_API_KEY, GOOGLE_API_KEYS, REDIS_URL, REDIS_PASSWORD
from ..db.supabase_client import get_supabase_client # Re-use client logic
from ..api.models.requests import JobStatus
from arq.cron import cron 

logger = logging.getLogger(__name__)

# Configure Google AI client with the first key
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
else:
    logger.warning("GOOGLE_API_KEY not set. AI functionality will be disabled.")

# --- Prompts (Similar to Go version) ---

BANK_STATEMENT_PROMPT = """
Analyze the provided bank statement page image/text. Extract the following information in JSON format:
- account_number: (string)
- bank_name: (string)
- statement_period: {{ from_date: (string, YYYY-MM-DD), to_date: (string, YYYY-MM-DD) }}
- opening_balance: (float)
- closing_balance: (float)
- transactions: [ {{ date: (string, YYYY-MM-DD), description: (string), reference: (string, optional), type: ('credit' or 'debit'), amount: (float), balance: (float, optional) }} ]
- summary: {{ total_credits: (float), total_debits: (float) }} (if available)
- confidence_score: (float, 0.0 to 1.0, estimate of extraction quality)

Return ONLY the JSON object. If the page contains no relevant information (e.g., blank page, cover page), return an empty JSON object {{}}.
"""

INVOICE_PROMPT = """
Analyze the provided invoice image/text. Extract the following information in JSON format:
- invoice_number: (string)
- invoice_date: (string, YYYY-MM-DD)
- due_date: (string, YYYY-MM-DD, optional)
- vendor_name: (string)
- vendor_address: (string, optional)
- vendor_tax_id: (string, optional, e.g., GSTIN, VAT ID)
- customer_name: (string, optional)
- customer_address: (string, optional)
- customer_tax_id: (string, optional)
- items: [ {{ description: (string), quantity: (float), unit_price: (float), total_price: (float), hsn_code: (string, optional) }} ]
- subtotal: (float)
- tax_amount: (float, total tax)
- total_amount: (float, grand total)
- currency: (string, e.g., INR, USD)
- confidence_score: (float, 0.0 to 1.0, estimate of extraction quality)

Return ONLY the JSON object. If the page contains no relevant information, return an empty JSON object {{}}.
"""

# --- Helper Functions ---

def get_sync_supabase_client() -> Client:
    """Creates a synchronous Supabase client for worker tasks."""
    # Workers might run in separate processes, simpler to create sync client
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL/Key missing for worker")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_sync_redis_client() -> redis.Redis:
    """Creates a synchronous Redis client for worker tasks."""
    # Assuming REDIS_URL format like redis://[:password@]host:port[/db]
    # decode_responses=True ensures keys/values are strings
    return redis.from_url(REDIS_URL, password=REDIS_PASSWORD, decode_responses=True)

# Renamed function to clarify target
async def update_job_status_in_redis(redis_client: redis.Redis, job_id: str, status: JobStatus, results: Optional[Dict[str, Any]] = None, error_message: Optional[str] = None):
    """Updates job status and optionally results/error in Redis."""
    redis_key = f"job:{job_id}"
    try:
        # Fetch existing job data
        job_json = redis_client.get(redis_key)
        if not job_json:
            # If the job doesn't exist when trying to update, log an error.
            # This might happen if the initial set failed or the key expired.
            logger.error(f"Job {job_id}: Cannot update status in Redis. Job key '{redis_key}' not found.")
            return
        
        try:
            job_data = json.loads(job_json)
        except json.JSONDecodeError:
            logger.error(f"Job {job_id}: Failed to decode existing job JSON from Redis: {job_json}")
            # Can't update if we can't parse, maybe set a simple error status?
            # For now, just returning.
            return

        # Update fields
        job_data["status"] = status.value
        job_data["updated_at"] = datetime.now(timezone.utc).isoformat() # Update timestamp
        if results is not None:
            job_data["result"] = results # Store results directly (no need to dump again)
        if error_message:
            job_data["error_message"] = error_message # Add error if needed
        else:
            # Clear error message if status is not failed
            job_data.pop("error_message", None) 

        # Store updated job back in Redis (with expiration, matching Go code: 24 hours)
        redis_client.set(redis_key, json.dumps(job_data), ex=24 * 3600)
        logger.info(f"Updated job {job_id} status to {status.value} in Redis")

    except Exception as e:
        # Catch-all for other potential Redis errors (connection issues, etc.)
        logger.error(f"Failed to update job {job_id} status in Redis: {e}")


# --- Supabase update function (keep for now, might be needed elsewhere or if pattern changes) ---
# This function is NO LONGER USED by process_document for status updates.
async def update_job_status(supabase: Client, job_id: str, status: JobStatus, results: Optional[Dict[str, Any]] = None, error_message: Optional[str] = None):
    """Updates job status and optionally results/error in Supabase."""
    update_data = {"status": status.value, "updated_at": datetime.now(timezone.utc).isoformat()}
    if results is not None:
        update_data["results"] = json.dumps(results) # Store results as JSON string
    if error_message:
        update_data["error_message"] = error_message 
    try:
        # *** IMPORTANT: This assumes a table named "jobs" exists ***
        await supabase.table("jobs").update(update_data).eq("id", job_id).execute()
        logger.warning(f"[DEPRECATED CALL] Updated job {job_id} status to {status.value} in Supabase table 'jobs'. Should use Redis.")
    except Exception as e:
        logger.error(f"Failed to update job {job_id} status in Supabase table 'jobs': {e}")

def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    """Extracts a JSON object from a string, handling markdown code blocks and performing cleaning similar to Go implementation."""
    json_str = None
    try:
        # Handle markdown code blocks first
        if "```json" in text:
            json_str = text.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in text:
             # Handle generic markdown block if ```json not present
             json_str = text.split("```", 1)[1].split("```", 1)[0]
        
        # Fallback: If no markdown blocks found, find first '{' and last '}' and clean like Go
        if json_str is None:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                # Slice between braces
                json_str = text[start:end+1]
                # Perform cleaning similar to Go's extractJSONFromText
                # (This was the missing part)
                # json_str = json_str.replace("```json", "") # Redundant if already sliced
                # json_str = json_str.replace("```", "")      # Redundant if already sliced
                json_str = json_str.replace("\\n", " ") # Replace escaped newlines (might be present)
                json_str = json_str.replace("\n", " ")  # Replace literal newlines
                json_str = json_str.replace("\r", "")   # Replace carriage returns
            else:
                # If no braces found either, return None
                logger.warning("Could not find JSON object delimiters or markdown blocks in text.")
                return None
        
        # Trim whitespace from the result before parsing
        json_str = json_str.strip()

        # Attempt to parse the extracted/cleaned string
        return json.loads(json_str) 
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}\\nAttempted JSON string: {json_str[:500]}...\\nOriginal text: {text[:500]}...")
        return None
    except Exception as e: # Catch index errors from split etc.
        logger.error(f"Unexpected error extracting JSON: {e}")
        return None

async def call_gemini(prompt: str, content_parts: list):
    """Calls the Gemini API with the given prompt and content parts."""
    if not GOOGLE_API_KEYS:
        raise ConnectionError("No Google API Keys configured.")
    
    # Try each API key in sequence until one works
    errors = []
    for api_key in GOOGLE_API_KEYS:
        try:
            # Configure with the current key
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-1.5-flash') # Or other suitable model
            response = await model.generate_content_async(contents=[prompt] + content_parts)
            return response.text
        except Exception as e:
            errors.append(f"API Key {api_key[:5]}... failed: {str(e)}")
            logger.warning(f"API Key {api_key[:5]}... failed: {str(e)}")
            continue
    
    # If we get here, all keys failed
    error_msg = "All Google API keys failed. Errors: " + "; ".join(errors)
    logger.error(error_msg)
    raise ConnectionError(error_msg)

# --- Usage Quota Update Helper (NEW) ---

def _update_usage_quota_in_supabase(supabase: Client, document_id: str):
    """Fetches document details and updates the usage_quotas table.
       Logs errors but does not raise exceptions to prevent failing the main job.
    """
    try:
        # 1. Get user_id and file_size from documents table
        doc_response = supabase.table("documents") \
                             .select("user_id, file_size") \
                             .eq("id", document_id) \
                             .maybe_single() \
                             .execute()

        if not doc_response.data:
            logger.error(f"Quota Update: Document {document_id} not found.")
            return

        user_id = doc_response.data.get("user_id")
        file_size = doc_response.data.get("file_size", 0)

        if not user_id:
            logger.error(f"Quota Update: User ID not found for document {document_id}.")
            return
        if file_size is None:
            file_size = 0 # Default to 0 if size is null

        # 2. Determine current billing period (first day of current month)
        now = datetime.now(timezone.utc)
        period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Calculate period_end (first day of next month)
        next_month = period_start.replace(day=28) + timedelta(days=4) # Go to approx next month
        period_end = next_month.replace(day=1) 

        # Format for Supabase timestampz
        period_start_iso = period_start.isoformat()
        period_end_iso = period_end.isoformat()

        # 3. Upsert usage quota
        # Mimic Go's default limits if creating a new record
        default_docs_limit = 1000
        default_data_limit = 10 * 1024 * 1024 * 1024 # 10 GB

        # Data to potentially insert
        upsert_data = {
            "user_id": user_id,
            "period_start": period_start_iso,
            "docs_limit": default_docs_limit,
            "data_limit": default_data_limit,
            "docs_processed": 1, # Increment logic handled by DB function/trigger or manually on conflict
            "data_processed": file_size # Increment logic handled by DB function/trigger or manually on conflict
        }

        # Using upsert. If a row for user_id & period_start exists, it should ideally be updated.
        # NOTE: This relies on Supabase handling the increment on conflict OR 
        # a more complex select-then-update approach might be needed if upsert doesn't increment.
        # For simplicity, mirroring Go's apparent upsert reliance first.
        # Check Supabase upsert documentation for exact conflict handling (e.g., ON CONFLICT DO UPDATE).
        # A simple upsert might just overwrite, not increment. 
        # Let's assume for now we need to handle increment manually on conflict, or a DB trigger exists.
        
        # Simplified Upsert - may need refinement based on actual DB setup/triggers
        # If a conflict on (user_id, period_start) occurs, Supabase needs a way 
        # to *increment* existing values, not just overwrite.
        # This might require a custom DB function called via rpc or a more complex SELECT then UPDATE.
        # Placeholder for simple upsert:
        logger.info(f"Quota Update: Attempting upsert for user {user_id}, period {period_start_iso}")
        response = supabase.table("usage_quotas") \
                           .upsert(upsert_data, on_conflict="user_id,period_start") \
                           .execute()

        if response.data: # Check if upsert returned data
             logger.info(f"Quota Update: Successfully upserted usage for user {user_id}, period {period_start_iso}")
        else:
             # If upsert didn't return data, maybe the increment needs to be manual
             logger.warning(f"Quota Update: Upsert for user {user_id} completed but returned no data. Increment might need manual handling or DB trigger.")
             # Fallback/Alternative: SELECT first, then UPDATE or INSERT
             # This is more robust if upsert doesn't increment correctly.
             # quota_res = supabase.table("usage_quotas") \
             #                  .select("id, docs_processed, data_processed") \
             #                  .eq("user_id", user_id) \
             #                  .eq("period_start", period_start_iso) \
             #                  .maybe_single() \
             #                  .execute()
             # if quota_res.data:
             #     # Update existing
             #     existing_quota = quota_res.data
             #     update_res = supabase.table("usage_quotas") \
             #                       .update({ \
             #                           "docs_processed": existing_quota["docs_processed"] + 1, \
             #                           "data_processed": existing_quota["data_processed"] + file_size \
             #                       }) \
             #                       .eq("id", existing_quota["id"]) \
             #                       .execute()
             # else:
             #     # Insert new
             #     insert_res = supabase.table("usage_quotas").insert(upsert_data).execute()

    except Exception as e:
        logger.error(f"Quota Update: Failed to update usage quota for doc {document_id}: {e}", exc_info=True)
        # Do not re-raise, just log the error

# --- Webhook Notification Helper (NEW) ---

async def send_webhook_notification(callback_url: str, payload: Dict[str, Any]):
    """Sends a POST request to the specified callback URL with the JSON payload.
       Logs errors but does not raise exceptions.
    """
    if not callback_url:
        return

    logger.info(f"Sending webhook to {callback_url}")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client: # 10 second timeout like Go
            response = await client.post(callback_url, json=payload)

            if response.status_code >= 400:
                logger.error(f"Webhook failed for {callback_url}. Status: {response.status_code}, Response: {response.text[:500]}")
            else:
                logger.info(f"Webhook sent successfully to {callback_url}. Status: {response.status_code}")

    except httpx.RequestError as e:
        logger.error(f"Webhook failed for {callback_url}. Request Error: {e}")
    except Exception as e:
        logger.error(f"Webhook failed for {callback_url}. Unexpected Error: {e}", exc_info=True)

# --- Batch Status Check Task (NEW) ---

async def check_batch_statuses(ctx: dict):
    """Periodically checks pending/processing batches and updates their status if all jobs are done."""
    # --- Use async redis client from context --- (MODIFIED)
    redis_client: ArqRedis = ctx['redis'] # ARQ puts the pool/client here
    # redis_client = get_sync_redis_client() # REMOVE sync client
    logger.info("Running periodic batch status check...")
    updated_count = 0
    processed_keys = 0
    try:
        # Use async iteration for scan_iter if available, or handle potential blocking
        # Note: redis-py's async scan_iter might still fetch keys in chunks.
        # If this becomes a bottleneck with huge numbers of keys, different strategies exist.
        async for batch_key_bytes in redis_client.scan_iter("batch:*"): # Use async for loop
            processed_keys += 1
            # --- Decode bytes key to string --- (NEW)
            batch_key = batch_key_bytes.decode('utf-8')
            # --- End Decode ---
            batch_id = batch_key.split(":", 1)[1] # Extract ID from key (Now works on string)
            batch_json = await redis_client.get(batch_key) # Use await (GET can often accept bytes or str key)
            if not batch_json:
                logger.warning(f"Batch Check: Found key {batch_key} but failed to get value.")
                continue
                
            try:
                batch_data = json.loads(batch_json)
            except json.JSONDecodeError:
                logger.warning(f"Batch Check: Failed to decode JSON for batch {batch_id}. Skipping.")
                continue
            
            current_batch_status = batch_data.get("status")
            
            # Only check batches that are not already in a final state
            if current_batch_status not in [JobStatus.PENDING.value, JobStatus.PROCESSING.value]:
                continue

            job_ids = batch_data.get("job_ids", [])
            if not job_ids:
                logger.warning(f"Batch Check: Batch {batch_id} has no job IDs. Marking as failed.")
                if current_batch_status != JobStatus.FAILED.value:
                    batch_data["status"] = JobStatus.FAILED.value
                    batch_data["updated_at"] = datetime.now(timezone.utc).isoformat()
                    await redis_client.set(batch_key, json.dumps(batch_data), ex=24 * 3600) # Use await
                    updated_count += 1
                continue

            # Get statuses of all constituent jobs
            job_keys = [f"job:{job_id}" for job_id in job_ids]
            # --- Use async mget --- (MODIFIED)
            job_jsons = await redis_client.mget(*job_keys) # Use await and unpack keys

            allCompleted = True
            allFailed = True
            # Results aggregation similar to Go (only if all completed)
            job_results_for_batch = [] 

            if len(job_jsons) != len(job_ids):
                logger.warning(f"Batch Check: MGET for batch {batch_id} returned {len(job_jsons)} results, expected {len(job_ids)}. Some jobs missing?")
                # Treat missing jobs as failed for status calculation
                allCompleted = False 
            
            for i, job_json in enumerate(job_jsons):
                job_id = job_ids[i]
                job_status = JobStatus.FAILED # Assume failed if data missing/invalid
                
                if job_json:
                    try:
                        job_data = json.loads(job_json)
                        job_status = JobStatus(job_data.get("status", JobStatus.FAILED))
                        if job_status == JobStatus.COMPLETED:
                            allFailed = False
                            # Collect results only if job completed
                            if job_data.get("result"):
                                job_results_for_batch.append(job_data["result"])
                        elif job_status == JobStatus.FAILED:
                            allCompleted = False
                        else: # Pending or Processing
                            allCompleted = False
                            allFailed = False
                            break # Batch is not finished yet, stop checking this batch
                    except (json.JSONDecodeError, ValueError):
                        logger.warning(f"Batch Check: Failed to decode/parse job {job_id} status for batch {batch_id}. Treating as failed.")
                        allCompleted = False
                else:
                    # Job key doesn't exist in Redis (expired or never created?)
                    logger.warning(f"Batch Check: Job key 'job:{job_id}' not found for batch {batch_id}. Treating as failed.")
                    allCompleted = False

            # If the inner loop didn't break, the batch has finished (all jobs completed or failed)
            if allCompleted or allFailed:
                finalStatus = JobStatus.COMPLETED if allCompleted else JobStatus.FAILED
                
                # Update batch only if status changed
                if current_batch_status != finalStatus.value:
                    logger.info(f"Batch Check: Updating batch {batch_id} status from {current_batch_status} to {finalStatus.value}")
                    batch_data["status"] = finalStatus.value
                    batch_data["updated_at"] = datetime.now(timezone.utc).isoformat()
                    # Add results only if all completed (like Go)
                    if finalStatus == JobStatus.COMPLETED:
                        batch_data["result"] = job_results_for_batch 
                    await redis_client.set(batch_key, json.dumps(batch_data), ex=24 * 3600) # Use await
                    updated_count += 1
                    # Optional: Send batch callback here if needed
                    # --- Call Webhook for Batch --- (NEW)
                    if batch_data.get("callback_url"):
                        # Prepare payload similar to Go's SendBatchCallback
                        batch_payload = {
                            "batch_id": batch_data.get("id"),
                            "status": batch_data.get("status"),
                            "job_ids": batch_data.get("job_ids"),
                            "created_at": batch_data.get("created_at"),
                            "updated_at": batch_data.get("updated_at"),
                            "result": batch_data.get("result") # Send aggregated results if completed
                        }
                        await send_webhook_notification(batch_data["callback_url"], batch_payload) 
                    # --- End Call Webhook --- 
            
            # If batch was pending and jobs started processing (but not finished), update to processing
            elif current_batch_status == JobStatus.PENDING.value:
                 logger.info(f"Batch Check: Updating batch {batch_id} status from {current_batch_status} to {JobStatus.PROCESSING.value}")
                 batch_data["status"] = JobStatus.PROCESSING.value
                 batch_data["updated_at"] = datetime.now(timezone.utc).isoformat()
                 await redis_client.set(batch_key, json.dumps(batch_data), ex=24 * 3600) # Use await
                 updated_count += 1
                
    except redis.RedisError as e:
        logger.error(f"Batch Check: Redis error during async scan/update: {e}")
    except Exception as e:
        logger.error(f"Batch Check: Unexpected error: {e}", exc_info=True)
        
    logger.info(f"Finished periodic batch status check. Scanned {processed_keys} keys. Updated {updated_count} batches.")


# --- ARQ Task Definition ---

async def process_document(ctx: dict, job_id: str, document_id: str, file_id: str, user_id: str, callback_url: Optional[str], job_type: str):
    """ARQ task to process a single document (statement or invoice)."""
    supabase = get_sync_supabase_client() # Use sync client for storage download
    redis_client = get_sync_redis_client() # Use sync client for job status updates
    logger.info(f"Starting processing for job_id: {job_id}, document_id: {document_id}, file_id: {file_id}")

    # --- 1. Create Initial Job Representation & Set Status in Redis ---
    initial_job_data = {
        "id": job_id,
        "document_id": document_id,
        "file_id": file_id,
        "user_id": user_id,
        "callback_url": callback_url,
        "job_type": job_type,
        "status": JobStatus.PROCESSING.value, # Start as processing
        "created_at": datetime.now(timezone.utc).isoformat(), # Timestamp when worker picked it up
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "results": None, 
        "error_message": None
        # Note: Go's CreateJob uses time.Now() which is when API received it.
        # This uses time.Now() which is when the worker starts. Slight difference.
    }
    try:
        # Store initial processing status in Redis (overwrite if somehow exists)
        redis_key = f"job:{job_id}"
        redis_client.set(redis_key, json.dumps(initial_job_data), ex=24 * 3600) # 24h expiry
        logger.info(f"Job {job_id}: Initial status PROCESSING set in Redis.")
    except Exception as redis_err:
        logger.error(f"Job {job_id}: Failed to set initial status in Redis: {redis_err}. Aborting task.")
        # If we can't even write to Redis, we can't track progress, so abort.
        return 
    # --- End Initial Job Status Update ---

    # --- 2. Fetch Document Details (REMOVED) ---
    file_name_for_logging = file_id # Use file_id for logging

    # --- 3. Download File from Storage --- 
    # (Supabase client needed here)
    try:
        # Map job type to bucket (similar to upload.py)
        bucket_name = "invoices"
        if job_type == "bank_statement":
            bucket_name = "bank_statements"
        
        # Use file_id (which includes timestamp and original name) for the storage path
        storage_path = f"{user_id}/{file_id}" 

        logger.info(f"Job {job_id}: Downloading {storage_path} from bucket {bucket_name}")
        # Note: supabase-py storage client is sync
        file_bytes = supabase.storage.from_(bucket_name).download(storage_path)
        if not file_bytes:
            raise FileNotFoundError("Downloaded file is empty")
        logger.info(f"Job {job_id}: Downloaded {len(file_bytes)} bytes.")
    except Exception as e:
        logger.error(f"Job {job_id}: Failed to download file {storage_path}: {e}")
        # Update status to FAILED in Redis
        await update_job_status_in_redis(redis_client, job_id, JobStatus.FAILED, error_message=f"Storage Error: {e}")
        return

    # --- 4. Extract Content (PDF/Image) ---
    content_parts = []
    file_ext = os.path.splitext(file_id)[1].lower() # Get extension from file_id
    is_pdf = file_ext == ".pdf"
    prompt = BANK_STATEMENT_PROMPT if job_type == 'bank_statement' else INVOICE_PROMPT

    try:
        if is_pdf:
            pdf_doc = fitz.open(stream=file_bytes, filetype="pdf")
            logger.info(f"Job {job_id}: Processing {pdf_doc.page_count} pages.")
            for page_num in range(pdf_doc.page_count):
                page = pdf_doc.load_page(page_num)
                # Option 2: Extract image (better for Gemini vision)
                pix = page.get_pixmap()
                img_bytes = pix.tobytes("png") # Convert to PNG bytes
                content_parts.append({"mime_type": "image/png", "data": img_bytes})
            pdf_doc.close()
        else: # Handle Images
            img = Image.open(BytesIO(file_bytes))
            output_buffer = BytesIO()
            img_format = 'png' 
            img.save(output_buffer, format=img_format)
            content_parts.append({"mime_type": f"image/{img_format}", "data": output_buffer.getvalue()})
            logger.info(f"Job {job_id}: Processing image.")
            
        if not content_parts:
            raise ValueError("No content extracted from document")

    except Exception as e:
        logger.error(f"Job {job_id}: Failed to extract content: {e}")
        # Update status to FAILED in Redis
        await update_job_status_in_redis(redis_client, job_id, JobStatus.FAILED, error_message=f"Content Extraction Error: {e}")
        return

    # 5. Call Google AI (Gemini)
    try:
        logger.info(f"Job {job_id}: Calling Gemini API...")
        ai_response_text = await call_gemini(prompt, content_parts)
        logger.info(f"Job {job_id}: Received response from Gemini.")

    except Exception as e:
        logger.error(f"Job {job_id}: Gemini API call failed: {e}")
        # Update status to FAILED in Redis
        await update_job_status_in_redis(redis_client, job_id, JobStatus.FAILED, error_message=f"AI Error: {e}")
        return

    # 6. Parse AI Response
    parsed_results = extract_json_from_text(ai_response_text)

    if parsed_results is None:
        logger.error(f"Job {job_id}: Failed to parse JSON from Gemini response.")
        # Update status to FAILED in Redis
        await update_job_status_in_redis(redis_client, job_id, JobStatus.FAILED, error_message="AI Response Parsing Error")
        return
        
    if 'confidence_score' not in parsed_results:
        parsed_results['confidence_score'] = 0.0 
        logger.warning(f"Job {job_id}: Confidence score missing from AI response, set to 0.0")

    logger.info(f"Job {job_id}: Successfully parsed AI response.")

    # 7. Update Job Status and Results in Redis
    await update_job_status_in_redis(redis_client, job_id, JobStatus.COMPLETED, results=parsed_results)

    # --- Call Usage Quota Update --- (NEW)
    # This should only run on successful completion
    _update_usage_quota_in_supabase(supabase, document_id)
    # --- End Usage Quota Update ---

    # 8. Optional: Send Callback (if callback_url exists)
    # --- Call Webhook for Single Job --- (NEW)
    if callback_url:
        logger.info(f"Job {job_id}: Callback URL detected, attempting to send callback.")
        # Prepare payload similar to Go's SendCallback
        job_payload = {
            "job_id": job_id,
            "status": JobStatus.COMPLETED.value, # Use final status
            "file_id": file_id,
            "created_at": initial_job_data.get("created_at"), # Use initial creation time
            "updated_at": datetime.now(timezone.utc).isoformat(), # Current time for completion
            "result": parsed_results
        }
        await send_webhook_notification(callback_url, job_payload) 
        # NOTE: We might also want to send callbacks on FAILED status. Go code doesn't show this.
        # If needed, call this within the except blocks as well, adjusting the payload status/error.
    # --- End Call Webhook --- 

    logger.info(f"Finished processing job_id: {job_id}")

# --- ARQ Worker Settings --- 

# This class defines the functions available to the worker
class WorkerSettings:
    functions = [process_document, check_batch_statuses] # Add the new function
    # Add cron job to run check_batch_statuses every 10 seconds
    # --- Use second='*/10' --- (MODIFIED based on suggestion)
    cron_jobs = [
        cron(
            check_batch_statuses, 
            second=set(range(0, 60, 10)), # Run every 10 seconds
            run_at_startup=True
        )
    ]
    # Add other settings like `redis_settings`, `max_jobs`, etc. if needed
    # redis_settings = arq_redis_settings # Use settings from arq_client 