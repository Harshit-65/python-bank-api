import logging
import os
import json
import random
from io import BytesIO
from typing import Dict, Any, Optional
import mimetypes
from datetime import datetime, timezone

import google.generativeai as genai
from supabase import create_client, Client
from PIL import Image
import fitz # PyMuPDF
import redis # Import the redis library

from ..config import SUPABASE_URL, SUPABASE_KEY, GOOGLE_API_KEY, GOOGLE_API_KEYS, REDIS_URL, REDIS_PASSWORD
from ..db.supabase_client import get_supabase_client # Re-use client logic
from ..api.models.requests import JobStatus

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
    """Extracts a JSON object from a string, handling markdown code blocks."""
    try:
        # Handle markdown code blocks
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
             text = text.split("```")[1].split("```")[0]
        
        # Find the first '{' and the last '}'
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            json_str = text[start:end+1]
            return json.loads(json_str)
        else:
            logger.warning("Could not find JSON object delimiters in text.")
            return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}\nRaw text: {text[:500]}...")
        return None
    except Exception as e:
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

    # 8. Optional: Send Callback (if callback_url exists)
    # You would need a function similar to Go's SendCallback here, using httpx
    if callback_url:
        logger.info(f"Job {job_id}: Callback URL detected, attempting to send callback.")
        # await send_callback_notification(job_id, JobStatus.COMPLETED, parsed_results, callback_url) # Implement this function if needed
        pass # Placeholder

    logger.info(f"Finished processing job_id: {job_id}")

# --- ARQ Worker Settings --- 

# This class defines the functions available to the worker
class WorkerSettings:
    functions = [process_document]
    # Add other settings like `redis_settings`, `max_jobs`, etc. if needed
    # redis_settings = arq_redis_settings # Use settings from arq_client 