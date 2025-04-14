import logging
import os
import json
import random
import re # Import regex for parsing amounts
from io import BytesIO
from typing import Dict, Any, Optional, List, Tuple
# Standard library imports
import asyncio
import json
import logging
import mimetypes
import os
import random
import re
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Dict, Any, Optional, List, Tuple
import uuid

# Third-party imports
import google.generativeai as genai
import httpx
import redis
from arq import ArqRedis, cron
from arq.connections import RedisSettings as ArqRedisSettings
from PIL import Image
# --- Use AsyncClient --- 
from supabase import create_async_client, AsyncClient 
# --- Remove sync Client import --- 
# from supabase import create_client, Client 
import fitz  # PyMuPDF

# Local imports
from ..config import (
    SUPABASE_URL, 
    SUPABASE_KEY, 
    GOOGLE_API_KEY, 
    GOOGLE_API_KEYS, 
    REDIS_URL, 
    REDIS_PASSWORD,
    RedisSettings as AppRedisSettings
)
# --- REMOVE get_supabase_client import from db, we create locally for now ---
# from ..db.supabase_client import get_supabase_client 
from ..api.models.requests import JobStatus
import time
from ..utils.schema import get_schema, SchemaNotFoundError, DatabaseOperationError # get_schema expects AsyncClient

logger = logging.getLogger(__name__)

# Configure Google AI client with the first key
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
else:
    logger.warning("GOOGLE_API_KEY not set. AI functionality will be disabled.")




# --- Constants ---
PAGE_RESULT_EXPIRY = 24 * 3600 # Expiry for temporary page results (seconds)
PAGE_COUNTER_EXPIRY = 24 * 3600 # Expiry for page counters

# --- Prompt Constants (Moved here for use in process_page) ---

DEFAULT_PROMPT_STATEMENT = """
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

DEFAULT_PROMPT_INVOICE = """
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

SCHEMA_PROMPT_TEMPLATE_INVOICE = """You are a precise invoice data extractor. Your task is to extract information from this invoice and format it EXACTLY according to the schema below. You MUST:

1. Follow the schema structure EXACTLY - do not add or modify any fields
2. Only include fields that are in the schema
3. Use empty values for fields not found (empty string for strings, 0 for numbers, empty arrays for arrays)
4. Do not add any fields that are not in the schema
5. Maintain the exact same field names as in the schema

SCHEMA:
%s

IMPORTANT: Your response must be EXACTLY in this format. Do not include ANY fields that are not in the schema, even if you find them in the invoice. Do not include any explanations or additional text - ONLY output the JSON that matches the schema structure.

Do not use this schema as just an example - this is the REQUIRED format that your response must follow. Your entire response should be valid JSON that can be parsed directly."""

SCHEMA_PROMPT_TEMPLATE_STATEMENT = """You are a precise bank statement data extractor. Your task is to extract information from this statement page and format it EXACTLY according to the schema below. You MUST:

1. Follow the schema structure EXACTLY - do not add or modify any fields
2. Only include fields that are in the schema
3. Use empty values for fields not found (empty string for strings, 0 for numbers, empty arrays for arrays)
4. Do not add any fields that are not in the schema
5. Maintain the exact same field names as in the schema

SCHEMA:
%s

IMPORTANT: Your response must be EXACTLY in this format. Do not include ANY fields that are not in the schema, even if you find them in the statement. Do not include any explanations or additional text - ONLY output the JSON that matches the schema structure.

Do not use this schema as just an example - this is the REQUIRED format that your response must follow. Your entire response should be valid JSON that can be parsed directly."""

# --- End Prompt Constants ---


# --- Helper Functions ---
# --- NEW Task: Process a Single Page ---
async def process_page(
    ctx: dict,
    job_id: str,
    document_id: str, # Pass document_id for quota update later
    file_id: str, # Pass file_id for webhook later
    user_id: str, # Pass user_id for potential future use
    job_type: str,
    schema_id: Optional[str], # <-- Add schema_id parameter
    page_num: int, # 0-indexed page number
    total_pages: int,
    page_image_bytes: bytes
    ):
    """Processes a single page image using Gemini."""
    redis_client = await get_arq_redis_from_ctx(ctx)
    # --- Get AsyncClient (create directly for now) --- 
    supabase_client: AsyncClient = await create_async_client(SUPABASE_URL, SUPABASE_KEY)
    page_start_time = time.time() # Start timer for page processing
    logger.info(f"Job {job_id}: Starting processing for page {page_num + 1}/{total_pages}, Schema: {schema_id or 'default'}")

    page_result_key = f"page_result:{job_id}:{page_num}"
    page_result_data = {"page_num": page_num + 1, "success": False, "error": None, "data": None}
    ai_response_text = None # Store raw response
    schema_string = None # Store fetched schema string

    try:
        # --- 1. Fetch Schema String (if schema_id provided) ---
        if schema_id:
            try:
                # Convert schema_id string to UUID
                schema_uuid = uuid.UUID(schema_id)
                # Fetch schema using the utility function (requires user_id)
                # We need the user_id as a UUID for the get_schema function
                user_uuid = uuid.UUID(user_id)
                # --- Pass the async client to get_schema --- 
                schema_db = await get_schema(supabase_client, schema_uuid, user_uuid)

                # --- Check Schema Type Mismatch --- 
                if schema_db.type.value != job_type:
                    logger.warning(f"Job {job_id}, Page {page_num + 1}: Schema type '{schema_db.type.value}' mismatches job type '{job_type}'. Using default prompt.")
                    schema_string = None # Force use of default prompt
                else:
                    schema_string = schema_db.schema_string
                    logger.info(f"Job {job_id}, Page {page_num + 1}: Using custom schema ID {schema_id} of type {schema_db.type.value}")
                # --- End Check ---
                
            except (SchemaNotFoundError, DatabaseOperationError, ValueError) as schema_err:
                logger.warning(f"Job {job_id}, Page {page_num + 1}: Failed to fetch/validate schema ID {schema_id}: {schema_err}. Using default prompt.")
                schema_string = None # Fallback to default
            except Exception as e:
                logger.error(f"Job {job_id}, Page {page_num + 1}: Unexpected error fetching schema {schema_id}: {e}. Using default prompt.", exc_info=True)
                schema_string = None
        # --- End Fetch Schema ---

        # --- 2. Prepare Prompt and Content --- 
        prompt = "" # Initialize prompt
        if schema_string:
            # Use the schema-specific prompt template
            prompt_template = SCHEMA_PROMPT_TEMPLATE_INVOICE if job_type == "invoice" else SCHEMA_PROMPT_TEMPLATE_STATEMENT
            prompt = prompt_template % schema_string
        else:
            # Use the default prompt
            prompt = DEFAULT_PROMPT_INVOICE if job_type == "invoice" else DEFAULT_PROMPT_STATEMENT

        # Prepare content parts (image)
        content_parts = [{"mime_type": "image/png", "data": page_image_bytes}]
        # --- End Prepare Prompt ---

        # 3. Call Gemini
        gemini_start_time = time.time()
        ai_response_text = await call_gemini(prompt, content_parts)
        gemini_duration = time.time() - gemini_start_time
        logger.info(f"Job {job_id}, Page {page_num + 1}: Received Gemini response in {gemini_duration:.2f}s.")
        logger.debug(f"Job {job_id}, Page {page_num + 1}: Raw Gemini Response:\n{ai_response_text}") # Log raw response

        # 4. Parse Response
        parsing_start_time = time.time()
        parsed_data = extract_json_from_text(ai_response_text) # extract_json now logs failures
        parsing_duration = time.time() - parsing_start_time
        if parsed_data is None:
            # Log the failure to parse more prominently here
            logger.error(f"Job {job_id}, Page {page_num + 1}: Failed to parse JSON from Gemini response text (parsing time: {parsing_duration:.2f}s).")
            # --- Implement Retry Logic ---
            logger.warning(f"Job {job_id}, Page {page_num + 1}: Retrying Gemini call once due to parse failure.")
            await asyncio.sleep(1) # Small delay before retry
            try:
                 ai_response_text = await call_gemini(prompt, content_parts) # Retry
                 logger.info(f"Job {job_id}, Page {page_num + 1}: Received Gemini response on retry.")
                 logger.debug(f"Job {job_id}, Page {page_num + 1}: Raw Gemini Response (Retry):\n{ai_response_text}")
                 parsed_data = extract_json_from_text(ai_response_text) # Try parsing again
                 if parsed_data is None:
                     logger.error(f"Job {job_id}, Page {page_num + 1}: Failed to parse JSON response even after retry.")
                     raise ValueError("Failed to parse JSON response after retry.")
                 else:
                     logger.info(f"Job {job_id}, Page {page_num + 1}: Successfully parsed JSON response on retry.")
            except Exception as retry_err:
                 logger.error(f"Job {job_id}, Page {page_num + 1}: Error during retry attempt: {retry_err}")
                 raise ValueError(f"Failed to parse JSON response after retry: {retry_err}")
            # --- End Retry Logic ---
        else:
             logger.info(f"Job {job_id}, Page {page_num + 1}: Successfully parsed JSON (parsing time: {parsing_duration:.2f}s).")

        # Log cleaned JSON if needed (DEBUG level)
        logger.debug(f"Job {job_id}, Page {page_num + 1}: Cleaned/Parsed JSON: {json.dumps(parsed_data)[:500]}...")

        # Ensure confidence score exists
        if isinstance(parsed_data, dict) and 'confidence_score' not in parsed_data:
            parsed_data['confidence_score'] = 0.0
            logger.warning(f"Job {job_id}, Page {page_num + 1}: Confidence score missing, set to 0.0")

        page_result_data["success"] = True
        page_result_data["data"] = parsed_data
        # logger.info(f"Job {job_id}: Successfully processed page {page_num + 1}") # Log moved after counter

    except Exception as e:
        logger.error(f"Job {job_id}: Failed to process page {page_num + 1}: {e}", exc_info=True)
        page_result_data["success"] = False
        page_result_data["error"] = str(e)
        if ai_response_text: # Include raw response in error if available
            page_result_data["raw_ai_response"] = ai_response_text


    # 4. Store Page Result in Redis
    try:
        await redis_client.set(page_result_key, json.dumps(page_result_data), ex=PAGE_RESULT_EXPIRY)
    except Exception as redis_err:
        logger.error(f"Job {job_id}, Page {page_num + 1}: Failed to store page result in Redis: {redis_err}")
        # Consider how critical this is - maybe try to update main job as failed?

    # 5. Increment Page Counter and Check for Completion
    page_counter_key = f"page_complete_count:{job_id}"
    try:
        completed_count = await redis_client.incr(page_counter_key)
        # Set expiry on first increment
        if completed_count == 1:
            await redis_client.expire(page_counter_key, PAGE_COUNTER_EXPIRY)

        page_duration = time.time() - page_start_time # Calculate total page time
        logger.info(f"Job {job_id}: Page {page_num + 1}/{total_pages} finished processing (Status: {'Success' if page_result_data['success'] else 'Failed'}). Duration: {page_duration:.2f}s. Completed count: {completed_count}/{total_pages}")

        # If this is the last page, enqueue the assembly task
        if completed_count >= total_pages:
            logger.info(f"Job {job_id}: All pages reported completion. Enqueuing assembly task.")
            await redis_client.enqueue_job(
                "assemble_job_results",
                _job_id=f"asm_{job_id}",
                job_id=job_id,
                document_id=document_id,
                file_id=file_id,
                user_id=user_id,
                job_type=job_type,
                schema_id=schema_id, # <-- Pass schema_id
                total_pages=total_pages
            )
    except Exception as e:
        logger.error(f"Job {job_id}, Page {page_num + 1}: Failed to increment/check page counter or enqueue assembly: {e}", exc_info=True)


def get_sync_redis_client() -> redis.Redis:
    """Creates a synchronous Redis client for worker tasks."""
    # Assuming REDIS_URL format like redis://[:password@]host:port[/db]
    # decode_responses=True ensures keys/values are strings
    return redis.from_url(REDIS_URL, password=REDIS_PASSWORD, decode_responses=True)

async def get_arq_redis_from_ctx(ctx: dict) -> ArqRedis:
    """Safely gets the ArqRedis instance from the context."""
    redis_client = ctx.get('redis')
    if not redis_client:
        # If not in context (e.g., running outside worker), create a new pool
        # This might happen if assemble_job_results is called directly for testing
        logger.warning("Redis client not found in ARQ context, creating new pool.")
        # Ensure RedisSettings is configured correctly for ARQ
        arq_redis_settings = ArqRedisSettings(
            host=AppRedisSettings.redis_url.split("://")[1].split(":")[0],
            port=int(AppRedisSettings.redis_url.split(":")[2].split("/")[0]),
            password=AppRedisSettings.redis_password,
        )
        # Make sure arq library is imported if not already
        import arq
        redis_client = await arq.create_pool(arq_redis_settings)
        # Note: This pool won't be automatically managed/closed by ARQ lifespan in this case.
    return redis_client


async def update_job_status_in_redis(
    ctx: Optional[dict], # Make context optional for broader use
    job_id: str,
    status: JobStatus,
    redis_client_override: Optional[ArqRedis | redis.Redis] = None, # Allow sync or async override type hint
    results: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
    total_pages: Optional[int] = None, # Add total_pages
    # --- NEW: Add optional fields for stub creation ---
    user_id_for_stub: Optional[str] = None,
    document_id_for_stub: Optional[str] = None,
    file_id_for_stub: Optional[str] = None,
    job_type_for_stub: Optional[str] = None,
    schema_id_for_stub: Optional[str] = None,
    created_at_for_stub: Optional[str] = None
    # --- END NEW ---
    ):
    """Updates job status and optionally results/error/total_pages in Redis.
       If the job key doesn't exist during a final update, attempts to create a stub
       using the `*_for_stub` parameters if provided.
    """
    redis_client = redis_client_override
    # Determine which client to use
    if not redis_client and ctx:
        redis_client = ctx.get('redis') # ARQ provides async client here
    if not redis_client:
        logger.warning("Falling back to sync Redis client for update_job_status_in_redis.")
        redis_client = get_sync_redis_client() 

    if not redis_client:
         logger.error(f"Job {job_id}: Could not obtain Redis client to update status.")
         return 

    is_async = isinstance(redis_client, ArqRedis)
    redis_key = f"job:{job_id}"
    
    try:
        # Fetch existing job data 
        job_json = None # Initialize
        if is_async:
            job_json = await redis_client.get(redis_key) 
        else:
            job_json = redis_client.get(redis_key)
        
        # --- ADD LOGGING: Log data read from Redis ---
        logger.debug(f"Job {job_id}: Read from Redis key '{redis_key}': {job_json}")
        # --- END LOGGING ---

        job_data = {} # Initialize
        if not job_json:
            # Attempt to create stub if final status update and key is missing
            if status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                 logger.warning(f"Job {job_id}: Job key '{redis_key}' not found during final update. Creating stub.")
                 # --- MODIFIED: Use optional params for stub ---
                 job_data = {
                     "id": job_id,
                     "status": status.value,
                     "updated_at": datetime.now(timezone.utc).isoformat(),
                     "results": results,
                     "error_message": error_message,
                     "total_pages": total_pages,
                     "user_id": user_id_for_stub, 
                     "document_id": document_id_for_stub, 
                     "file_id": file_id_for_stub,
                     "job_type": job_type_for_stub,
                     "schema_id": schema_id_for_stub,
                     "created_at": created_at_for_stub or datetime.now(timezone.utc).isoformat()
                 }
                 missing_stub_fields = [k for k, v in job_data.items() if v is None and k.endswith('_id')]
                 if missing_stub_fields:
                     logger.warning(f"Job {job_id}: Missing fields in created stub: {missing_stub_fields}")
                 # --- END MODIFIED ---
            else:
                 logger.error(f"Job {job_id}: Cannot update non-final status in Redis. Job key '{redis_key}' not found.")
                 return
        else:
            # Parse existing data if found
            try:
                job_data = json.loads(job_json)
                # --- ADD LOGGING: Log parsed existing data ---
                logger.debug(f"Job {job_id}: Parsed existing data: {json.dumps(job_data)}")
                # --- END LOGGING ---
            except json.JSONDecodeError:
                logger.error(f"Job {job_id}: Failed to decode existing job JSON from Redis: {job_json}")
                # Overwrite with error status 
                job_data = { 
                     "id": job_id,
                     "user_id": user_id_for_stub, 
                     "status": JobStatus.FAILED.value,
                     "updated_at": datetime.now(timezone.utc).isoformat(),
                     "results": None,
                     "error_message": "Internal Error: Corrupted job data in cache.",
                }
                status = JobStatus.FAILED 

        # Update fields (on existing or newly created stub)
        job_data["status"] = status.value
        job_data["updated_at"] = datetime.now(timezone.utc).isoformat()
        if results is not None:
            # Ensure results are JSON serializable (basic check)
            try:
                json.dumps(results)
                job_data["result"] = results
            except TypeError as json_err:
                 logger.error(f"Job {job_id}: Results are not JSON serializable: {json_err}. Storing error message instead.")
                 job_data["result"] = None
                 job_data["error_message"] = f"Internal Error: Failed to serialize results - {json_err}"
                 status = JobStatus.FAILED # Mark as failed if results can't be stored
                 job_data["status"] = status.value
                 
        if error_message:
            job_data["error_message"] = error_message
        elif status == JobStatus.COMPLETED: # Clear error ONLY if explicitly completing successfully
            job_data.pop("error_message", None)

        if total_pages is not None: 
             job_data["total_pages"] = total_pages
        
        # --- MODIFIED: Always ensure essential IDs are present if provided via *_for_stub --- 
        if user_id_for_stub:
            job_data["user_id"] = user_id_for_stub
        if document_id_for_stub:
            job_data["document_id"] = document_id_for_stub
        if file_id_for_stub:
            job_data["file_id"] = file_id_for_stub
        if job_type_for_stub:
             job_data["job_type"] = job_type_for_stub
        if schema_id_for_stub:
             job_data["schema_id"] = schema_id_for_stub
        if created_at_for_stub: # Especially important for stubs
             job_data["created_at"] = job_data.get("created_at", created_at_for_stub) # Preserve existing if possible, else use stub
        # --- END MODIFIED ---

        # --- ADD LOGGING: Log final data before setting ---
        logger.debug(f"Job {job_id}: Final job_data before setting back to Redis: {json.dumps(job_data)}")
        # --- END LOGGING ---

        # Store updated job back in Redis 
        try:
            final_job_json = json.dumps(job_data)
        except TypeError as final_json_err:
            logger.error(f"Job {job_id}: CRITICAL - Failed to serialize final job_data before setting to Redis: {final_json_err}. Job data: {job_data}")
            # Avoid writing corrupted data if possible
            # Maybe try updating only status to FAILED? Difficult recovery path.
            return 

        if is_async:
             await redis_client.set(redis_key, final_job_json, ex=PAGE_RESULT_EXPIRY) 
        else:
             redis_client.set(redis_key, final_job_json, ex=PAGE_RESULT_EXPIRY)
        logger.info(f"Updated job {job_id} status to {status.value} in Redis")

    except Exception as e:
        logger.error(f"Failed to update job {job_id} status in Redis: {e}", exc_info=True) 



def _parse_amount(amount_input: Optional[str | float | int]) -> float:
    """Converts string/numeric amount to float, handling commas and Indian number format.
       Mimics Go's parseAmount function. Now handles numeric inputs directly.
    """
    # Handle numeric types directly
    if isinstance(amount_input, (int, float)):
        return float(amount_input)

    # Handle None or non-string types (after checking numeric)
    if not isinstance(amount_input, str):
        return 0.0

    # Proceed with string processing
    amount_str = amount_input # Now we know it's a string

    # Remove commas, spaces, and newlines
    clean_amount = amount_str.replace(",", "").replace(" ", "").replace("\\n", "")

    # Handle potential Indian format (e.g., 1.23.456,78 -> 123456.78)
    # Check if multiple periods exist before the potential last decimal part
    if "." in clean_amount:
        parts = clean_amount.split('.')
        if len(parts) > 2:
            # More than one dot, assume Indian grouping, rejoin main part, keep last part
            last_part = parts[-1]
            main_part = "".join(parts[:-1])
            clean_amount = main_part + "." + last_part

    # Use regex to find the first valid number pattern (integer or float)
    # This helps ignore surrounding text if any
    match = re.search(r'(-?(\\d+\\.?\\d*|\\.\\d+))|(-?\\d+)', clean_amount)
    if not match:
         logger.warning(f"_parse_amount: Could not find valid number pattern in '{amount_str}' (cleaned: '{clean_amount}')")
         return 0.0

    clean_amount = match.group(0)

    try:
        val = float(clean_amount)
        return val
    except ValueError as e:
        logger.warning(f"_parse_amount: Failed to parse amount '{amount_str}' (cleaned to '{clean_amount}'): {e}")
        return 0.0 


def _correct_transactions(transactions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """Iterates transactions, fixes number format, and corrects credit/debit based on balance changes.
       Mimics Go's correctTransactions function.
    """
    corrections = 0
    if not transactions:
        return transactions, 0

    # Fix Indian number format issues first
    for i in range(len(transactions)):
        # Ensure it's a dictionary before passing
        if isinstance(transactions[i], dict):
            _fix_indian_number_format(transactions[i])
        else:
            logger.warning(f"_correct_transactions: Item at index {i} is not a dict, skipping format fix.")
            # Decide how to handle non-dict items - skip or try to convert?
            # For now, skipping.

    # Get initial balance from the first transaction after format fixing
    prev_balance = 0.0
    if isinstance(transactions[0], dict):
         # Attempt to get balance, parse it, handle potential key errors
         try:
             prev_balance = _parse_amount(transactions[0].get("balance"))
         except Exception as e:
             logger.warning(f"Cannot determine initial balance, error parsing first item's balance: {e}")
             return transactions, 0 # Cannot proceed reliably
    else:
        logger.warning("Cannot determine initial balance, first item is not a dict.")
        return transactions, 0 # Cannot proceed reliably

    # Process each transaction
    for i in range(len(transactions)):
        # Ensure it's a dictionary
        if not isinstance(transactions[i], dict):
            logger.warning(f"_correct_transactions: Item at index {i} is not a dict, skipping correction logic.")
            continue

        current_balance_str = transactions[i].get("balance")
        current_balance = _parse_amount(current_balance_str)

        # If current balance couldn't be parsed reliably, skip this transaction's correction logic
        # but still update prev_balance if possible for the *next* iteration.
        if current_balance_str is not None and current_balance == 0.0 and current_balance_str not in ("0", "0.0", "0.00"):
             logger.warning(f"_correct_transactions: Unreliable balance parse ('{current_balance_str}' -> {current_balance}) at index {i}, skipping type correction.")
             prev_balance = current_balance # Still update prev_balance for next check
             continue

        balance_change = current_balance - prev_balance

        # Get current credit/debit strings (they might be None)
        credit_str = transactions[i].get("credit")
        debit_str = transactions[i].get("debit")
        # Parse amounts (will be 0.0 if string is None or invalid)
        credit_val = _parse_amount(credit_str)
        debit_val = _parse_amount(debit_str)

        corrected = False
        # Check if the balance change suggests a mismatch
        # Use a small threshold for float comparison (e.g., 0.01 for currency)
        if abs(balance_change) > 0.01:
            # Check for potential correction only if ONE of credit/debit has a non-zero value
            if debit_val > 0 and credit_val == 0 and balance_change > 0: # Should be credit
                transactions[i]["credit"] = debit_str # Keep original string representation
                transactions[i].pop("debit", None) # Remove debit field
                transactions[i]["type"] = "credit"
                corrected = True
            elif credit_val > 0 and debit_val == 0 and balance_change < 0: # Should be debit
                transactions[i]["debit"] = credit_str # Keep original string representation
                transactions[i].pop("credit", None) # Remove credit field
                transactions[i]["type"] = "debit"
                corrected = True

        # If a correction happened, log it
        if corrected:
            corrections += 1
            logger.info(f"_correct_transactions: Corrected type for transaction at index {i} (Description: {transactions[i].get('description')})")

        # Update previous balance for the next iteration
        prev_balance = current_balance

    return transactions, corrections


# --- Webhook Notification Helper ---
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


# --- Supabase update function (keep for now, might be needed elsewhere or if pattern changes) ---
# This function is NO LONGER USED by process_document for status updates.
async def update_job_status(supabase: AsyncClient, job_id: str, status: JobStatus, results: Optional[Dict[str, Any]] = None, error_message: Optional[str] = None):
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

# --- Usage Quota Update Helper (REFACTORED to ASYNC) --- 
async def update_usage_quota_in_supabase_async(supabase: AsyncClient, document_id: str):
    """Fetches document details and updates the usage_quotas table (Async version)."""
    try:
        # 1. Get user_id and file_size - USE AWAIT
        doc_response = await supabase.table("documents") \
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
            file_size = 0

        # 2. Determine current billing period
        now = datetime.now(timezone.utc)
        period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Format for Supabase timestampz
        period_start_iso = period_start.isoformat()

        # 3. Upsert usage quota - USE AWAIT
        default_docs_limit = 1000
        default_data_limit = 10 * 1024 * 1024 * 1024 
        upsert_data = {
            "user_id": user_id,
            "period_start": period_start_iso,
            "docs_limit": default_docs_limit,
            "data_limit": default_data_limit,
            "docs_processed": 1,
            "data_processed": file_size
        }
        
        logger.info(f"Quota Update: Attempting upsert for user {user_id}, period {period_start_iso}")
        # --- Use AWAIT --- 
        response = await supabase.table("usage_quotas") \
                           .upsert(upsert_data, on_conflict="user_id,period_start") \
                           .execute()

        if response.data: 
             logger.info(f"Quota Update: Successfully upserted usage for user {user_id}, period {period_start_iso}")
        else:
             logger.warning(f"Quota Update: Upsert for user {user_id} completed but returned no data. Check DB triggers/functions for increment logic.")

    except Exception as e:
        logger.error(f"Quota Update: Failed to update usage quota for doc {document_id}: {e}", exc_info=True)
# --- End Refactored Quota Function ---

# --- Batch Status Check Task (MODIFIED for async redis) ---

async def check_batch_statuses(ctx: dict):
    """Periodically checks pending/processing batches and updates their status if all jobs are done."""
    # --- Use async redis client from context --- (MODIFIED)
    redis_client: ArqRedis = ctx['redis'] # ARQ puts the pool/client here
    # redis_client = get_sync_redis_client() # REMOVE sync client
    # logger.info("Running periodic batch status check...")
    updated_count = 0
    processed_keys = 0
    try:
        # Use async iteration for scan_iter 
        async for batch_key_bytes in redis_client.scan_iter("batch:*"):
            processed_keys += 1
            # --- Decode bytes key to string --- (NEW)
            batch_key = batch_key_bytes.decode('utf-8')
            # --- End Decode ---
            batch_id = batch_key.split(":", 1)[1] # Extract ID from key
            batch_json = await redis_client.get(batch_key) # Use await
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
        
    # logger.info(f"Finished periodic batch status check. Scanned {processed_keys} keys. Updated {updated_count} batches.")

# --- Data Cleaning Helpers (Ported from Go response.go) ---

def _fix_indian_number_format(transaction: Dict[str, Any]) -> None:
    """Corrects balance, credit, debit fields with incorrect decimal points.
       Modifies the dictionary in-place. Mimics Go's fixIndianNumberFormat.
    """
    fields_to_check = ["balance", "credit", "debit"]
    for field in fields_to_check:
        value = transaction.get(field)
        # Check if value is a string and contains multiple dots
        if isinstance(value, str) and value.count('.') > 1:
            parts = value.split('.')
            # Assume last part is decimal, join others
            last_part = parts[-1]
            main_part = "".join(parts[:-1])
            corrected_value = main_part + "." + last_part
            transaction[field] = corrected_value
            logger.debug(f"_fix_indian_number_format: Corrected field '{field}' from '{value}' to '{corrected_value}'")


async def assemble_job_results(
    ctx: dict,
    job_id: str,
    document_id: str,
    file_id: str,
    user_id: str,
    job_type: str,
    schema_id: Optional[str], # <-- Add schema_id parameter
    total_pages: int
    ):
    """Gathers page results, merges them, cleans data, updates final job status."""
    redis_client = await get_arq_redis_from_ctx(ctx)
    # --- Get AsyncClient (create directly for now) --- 
    supabase_client: AsyncClient = await create_async_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info(f"Job {job_id}: Starting assembly of {total_pages} pages. Schema: {schema_id or 'default'}")

    all_page_results = []
    page_keys = [f"page_result:{job_id}:{i}" for i in range(total_pages)]
    page_error_count = 0
    final_results = {}
    final_status = JobStatus.FAILED # Assume failed unless proven otherwise
    error_message = None

    # -- Initialize variables needed for potential stub creation --
    user_id_for_stub = user_id
    document_id_for_stub = document_id
    file_id_for_stub = file_id
    job_type_for_stub = job_type
    schema_id_for_stub = schema_id # <-- Initialize schema_id for stub
    initial_created_at = datetime.now(timezone.utc).isoformat() 
    callback_url = None
    # -- End Initialize --

    try:
        # -- Attempt to read initial job data --
        try:
            job_data_json = await redis_client.get(f"job:{job_id}")
            if job_data_json:
                initial_job_data = json.loads(job_data_json)
                callback_url = initial_job_data.get("callback_url")
                initial_created_at = initial_job_data.get("created_at", initial_created_at)
                user_id_for_stub = initial_job_data.get("user_id", user_id_for_stub)
                document_id_for_stub = initial_job_data.get("document_id", document_id_for_stub)
                file_id_for_stub = initial_job_data.get("file_id", file_id_for_stub)
                job_type_for_stub = initial_job_data.get("job_type", job_type_for_stub)
                schema_id_for_stub = initial_job_data.get("schema_id", schema_id_for_stub) # <-- Update schema_id for stub
            else:
                 logger.warning(f"Job {job_id}: Initial job data not found in Redis during assembly start.")
        except json.JSONDecodeError:
            logger.error(f"Job {job_id}: Failed to decode initial job data during assembly start.")
        except redis.RedisError as redis_err:
             logger.error(f"Job {job_id}: Redis error fetching initial job data during assembly start: {redis_err}")
        # -- End attempt to read initial job data --

        # 1. Fetch all page results
        page_jsons = await redis_client.mget(*page_keys)

        if len(page_jsons) != total_pages or any(p is None for p in page_jsons):
            missing_count = total_pages - sum(1 for p in page_jsons if p is not None)
            logger.error(f"Job {job_id}: Missing {missing_count} page results during assembly. Expected {total_pages}.")
            page_error_count += missing_count
            page_jsons = [p for p in page_jsons if p is not None] # Filter out None entries

        # 2. Parse and validate page results
        for i, page_json in enumerate(page_jsons):
            try:
                page_data = json.loads(page_json)
                if not page_data.get("success"):
                    page_error_count += 1
                    logger.warning(f"Job {job_id}: Page {page_data.get('page_num', i+1)} reported failure: {page_data.get('error')}")
                all_page_results.append(page_data)
            except json.JSONDecodeError:
                page_error_count += 1
                page_num_approx = i + 1 # Best guess at page number
                logger.error(f"Job {job_id}: Failed to decode JSON for page result near index {i}.")
                all_page_results.append({"page_num": page_num_approx, "success": False, "error": "Failed to decode page result JSON"})


        # 3. Merge Results (if no errors during page processing)
        if page_error_count == 0:
            logger.info(f"Job {job_id}: All pages processed successfully. Merging results.")
            merged_data = {}
            all_transactions = []
            confidence_scores = []

            all_page_results.sort(key=lambda p: p.get("page_num", 0)) # Sort by page number

            for page_result in all_page_results:
                page_num = page_result.get("page_num")
                page_content = page_result.get("data")

                if page_content is None or not isinstance(page_content, dict):
                     logger.warning(f"Job {job_id}: Page {page_num} data is missing or not a dict, skipping merge.")
                     continue # Skip empty or invalid pages

                if page_content:
                     merged_data[f"page_{page_num}"] = page_content

                page_transactions = page_content.get("transactions")
                if isinstance(page_transactions, list):
                    all_transactions.extend(page_transactions)

                page_score = page_content.get("confidence_score")
                if isinstance(page_score, (float, int)):
                    confidence_scores.append(float(page_score))

            if all_transactions:
                merged_data["all_transactions"] = all_transactions
            if confidence_scores:
                merged_data["average_confidence_score"] = sum(confidence_scores) / len(confidence_scores)
            merged_data["total_pages"] = total_pages
            merged_data["pages_with_data"] = len(confidence_scores)

            # 4. Apply Data Cleaning (for bank statements)
            if job_type == "bank_statement":
                 logger.info(f"Job {job_id}: Applying bank statement transaction corrections.")
                 try:
                    if isinstance(merged_data.get("all_transactions"), list):
                        corrected_list, count = _correct_transactions(merged_data["all_transactions"])
                        merged_data["all_transactions"] = corrected_list
                        if count > 0: logger.info(f"Job {job_id}: Applied {count} corrections to all_transactions.")

                    for key in list(merged_data.keys()):
                        if key.startswith("page_") and isinstance(merged_data[key], dict):
                            page_data = merged_data[key]
                            if isinstance(page_data.get("transactions"), list):
                                corrected_page_list, page_count = _correct_transactions(page_data["transactions"])
                                page_data["transactions"] = corrected_page_list
                 except Exception as clean_err:
                     logger.error(f"Job {job_id}: Error during transaction cleaning: {clean_err}", exc_info=True)


            final_results = merged_data
            final_status = JobStatus.COMPLETED
            logger.info(f"Job {job_id}: Assembly and cleaning complete.")

        else:
            logger.error(f"Job {job_id}: Assembly failed due to {page_error_count} page errors.")
            final_status = JobStatus.FAILED
            error_message = f"{page_error_count}/{total_pages} pages failed processing."
            # Store raw page results if assembly failed, useful for debugging
            page_results_for_error = [p for p in all_page_results if not p.get('success')]
            final_results = {
                "page_processing_errors": page_results_for_error,
                "error_summary": error_message
            }

        # 5. Update Final Job Status in Redis (passing stub variables defined at the start)
        await update_job_status_in_redis(
            ctx,
            job_id,
            final_status, 
            redis_client_override=redis_client,
            results=final_results,
            error_message=error_message,
            total_pages=total_pages,
            user_id_for_stub=user_id_for_stub,
            document_id_for_stub=document_id_for_stub,
            file_id_for_stub=file_id_for_stub,
            job_type_for_stub=job_type_for_stub,
            schema_id_for_stub=schema_id_for_stub, # <-- Pass schema_id for stub
            created_at_for_stub=initial_created_at 
        )

        # 6. Update Quota and Send Webhook (if successful)
        if final_status == JobStatus.COMPLETED:
            # --- Call the new async version --- 
            await update_usage_quota_in_supabase_async(supabase_client, document_id)
            if callback_url:
                 job_payload = {
                     "job_id": job_id, "status": final_status.value, "file_id": file_id,
                     "schema_id": schema_id, # <-- Add schema_id to webhook payload
                     "created_at": initial_created_at, 
                     "updated_at": datetime.now(timezone.utc).isoformat(),
                     "result": final_results
                 }
                 await send_webhook_notification(callback_url, job_payload)

    except Exception as e:
        logger.error(f"Job {job_id}: Unexpected error during assembly: {e}", exc_info=True)
        # Update status to FAILED, passing stub data
        try:
            await update_job_status_in_redis(
                ctx,
                job_id,
                JobStatus.FAILED,
                redis_client_override=redis_client,
                error_message=f"Assembly Error: {e}",
                total_pages=total_pages,
                user_id_for_stub=user_id_for_stub,
                document_id_for_stub=document_id_for_stub,
                file_id_for_stub=file_id_for_stub,
                job_type_for_stub=job_type_for_stub,
                schema_id_for_stub=schema_id_for_stub, # <-- Pass schema_id for stub
                created_at_for_stub=initial_created_at
            )
        except Exception as final_update_err:
             logger.error(f"Job {job_id}: CRITICAL - Failed to mark job as failed after assembly error: {final_update_err}")

    finally:
        # 7. Cleanup temporary Redis keys
        logger.info(f"Job {job_id}: Cleaning up temporary Redis keys.")
        keys_to_delete = page_keys + [f"page_complete_count:{job_id}"]
        try:
            async with redis_client.pipeline(transaction=False) as pipe:
                 for key in keys_to_delete:
                     pipe.delete(key)
                 await pipe.execute()
        except Exception as e:
            logger.error(f"Job {job_id}: Failed to clean up Redis keys {keys_to_delete}: {e}")


    logger.info(f"Job {job_id}: Assembly finished with status {final_status.value}.") 


# --- ARQ Task Definition (Modified) ---


async def process_document(
    ctx: dict,
    job_id: str, 
    document_id: str, 
    file_id: str, 
    user_id: str,
    callback_url: Optional[str], 
    job_type: str,
    schema_id: Optional[str] = None # <-- Add schema_id parameter
    ):
    """ARQ task to initiate processing by splitting document into page tasks."""
    # --- Get Async Client (create directly for now) ---
    supabase_client: AsyncClient = await create_async_client(SUPABASE_URL, SUPABASE_KEY)
    redis_client = await get_arq_redis_from_ctx(ctx) # Use async client for ARQ enqueue
    logger.info(f"Job {job_id}: Initiating processing (scatter phase) for doc {document_id} file {file_id} schema {schema_id or 'default'}")

    # --- 1. Create Initial Job Record & Set Status ---
    initial_job_data = {
        "id": job_id, "document_id": document_id, "file_id": file_id,
        "user_id": user_id, "callback_url": callback_url, "job_type": job_type,
        "schema_id": schema_id, # <-- Store schema_id
        "status": JobStatus.PENDING.value, # Start as pending (pages not dispatched yet)
        "created_at": datetime.now(timezone.utc).isoformat(), # Timestamp when scatter task starts
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "results": None, "error_message": None, "total_pages": None
    }
    try:
        # --- ADD LOGGING: Log initial data before setting ---
        logger.debug(f"Job {job_id}: Setting initial job data: {json.dumps(initial_job_data)}")
        # --- END LOGGING ---
        # Use await with the async client from context
        await redis_client.set(f"job:{job_id}", json.dumps(initial_job_data), ex=PAGE_RESULT_EXPIRY)
        logger.info(f"Job {job_id}: Initial status PENDING set in Redis.")
    except Exception as redis_err:
        logger.error(f"Job {job_id}: Failed to set initial status in Redis: {redis_err}. Aborting.", exc_info=True)
        return # Cannot proceed

    # --- NEW: Fetch Document Type from DB --- 
    actual_doc_type = None
    try:
        doc_response = await supabase_client.table("documents") \
                                       .select("document_type") \
                                       .eq("id", document_id) \
                                       .eq("user_id", user_id) \
                                       .maybe_single() \
                                       .execute()
        if doc_response.data and doc_response.data.get("document_type"):
            actual_doc_type = doc_response.data["document_type"]
            logger.info(f"Job {job_id}: Fetched actual document type from DB: {actual_doc_type}")
        else:
            # If document not found or type missing, this is a critical error
            raise FileNotFoundError(f"Document {document_id} not found or missing type in database for user {user_id}.")
    except Exception as db_err:
        logger.error(f"Job {job_id}: Failed to fetch document type from database: {db_err}", exc_info=True)
        await update_job_status_in_redis(
            ctx, job_id, JobStatus.FAILED,
            redis_client_override=redis_client,
            error_message=f"DB Error fetching doc type: {db_err}",
            # Pass necessary stub data
            user_id_for_stub=user_id, document_id_for_stub=document_id,
            file_id_for_stub=file_id, job_type_for_stub=job_type,
            schema_id_for_stub=schema_id
        )
        return
    # --- END Fetch Document Type --- 

    # --- 2. Download File --- 
    file_bytes = None
    try:
        # --- Use actual_doc_type to determine bucket --- 
        bucket_name = "invoices" if actual_doc_type == "invoice" else "bank_statements"
        # --- End Use actual_doc_type ---
        # IMPORTANT: Use file_id for storage path as per upload logic
        storage_path = f"{user_id}/{file_id}"
        logger.info(f"Job {job_id}: Downloading {storage_path} from bucket {bucket_name} (based on DB type '{actual_doc_type}')")
        # --- Assuming async storage client usage --- 
        storage_response = await supabase_client.storage.from_(bucket_name).download(storage_path)
        file_bytes = storage_response
        if not file_bytes: raise FileNotFoundError(f"Downloaded file is empty: {storage_path}")
        logger.info(f"Job {job_id}: Downloaded {len(file_bytes)} bytes.")
    except Exception as e:
        logger.error(f"Job {job_id}: Failed to download file {storage_path} from bucket {bucket_name}: {e}", exc_info=True)
        # Update status with error (stub data is passed correctly)
        await update_job_status_in_redis(
            ctx, job_id, JobStatus.FAILED, 
            redis_client_override=redis_client, 
            error_message=f"Storage Download Error: {e}",
            user_id_for_stub=user_id, document_id_for_stub=document_id,
            file_id_for_stub=file_id, job_type_for_stub=actual_doc_type, # Use actual type for stub
            schema_id_for_stub=schema_id
        )
        return
    
    # --- 3. Extract Pages and Enqueue Page Tasks ---
    num_pages = 0
    page_tasks_enqueued = 0
    try:
        file_ext = os.path.splitext(file_id)[1].lower() # Get extension from file_id
        if file_ext == ".pdf":
            pdf_doc = fitz.open(stream=file_bytes, filetype="pdf")
            num_pages = pdf_doc.page_count
            logger.info(f"Job {job_id}: PDF has {num_pages} pages. Enqueuing page tasks...")

            if num_pages == 0:
                 raise ValueError("PDF document has 0 pages.")

            # Update job record with total pages before enqueuing
            await update_job_status_in_redis(
                ctx, 
                job_id, 
                JobStatus.PENDING, # Keep status PENDING for now
                redis_client_override=redis_client, 
                total_pages=num_pages,
                user_id_for_stub=user_id,        # Pass user_id
                document_id_for_stub=document_id, # Pass doc_id
                file_id_for_stub=file_id,         # Pass file_id
                job_type_for_stub=job_type,        # Pass job_type
                schema_id_for_stub=schema_id,      # Pass schema_id
            )

            for page_num in range(num_pages):
                page = pdf_doc.load_page(page_num)
                pix = page.get_pixmap(dpi=150) # Extract with reasonable DPI
                img_bytes = pix.tobytes("png") # Convert to PNG bytes

                # Enqueue process_page task
                await redis_client.enqueue_job(
                    "process_page",
                    _job_id=f"page_{job_id}_{page_num}", # Make ARQ job ID unique per page
                    job_id=job_id,
                    document_id=document_id,
                    file_id=file_id,
                    user_id=user_id,
                    job_type=job_type,
                    schema_id=schema_id, # <-- Pass schema_id to page task
                    page_num=page_num,
                    total_pages=num_pages,
                    page_image_bytes=img_bytes,
                )
                page_tasks_enqueued += 1
            pdf_doc.close()
            logger.info(f"Job {job_id}: Enqueued {page_tasks_enqueued} page tasks.")

        else: # Handle single-page images
            num_pages = 1
            logger.info(f"Job {job_id}: Processing single image. Enqueuing page task...")
            # Update job record with total pages
            await update_job_status_in_redis(ctx, job_id, JobStatus.PENDING, redis_client_override=redis_client, total_pages=num_pages)
            # Enqueue just one process_page task
            await redis_client.enqueue_job(
                "process_page",
                 _job_id=f"page_{job_id}_0",
                 job_id=job_id, document_id=document_id, file_id=file_id, user_id=user_id,
                 job_type=job_type, 
                 schema_id=schema_id, # <-- Pass schema_id to page task
                 page_num=0, total_pages=1, page_image_bytes=file_bytes
            )
            page_tasks_enqueued = 1
            logger.info(f"Job {job_id}: Enqueued 1 page task.")

        # Update status to PROCESSING (passing stub IDs)
        if page_tasks_enqueued > 0:
             await update_job_status_in_redis(
                 ctx, job_id, JobStatus.PROCESSING, 
                 redis_client_override=redis_client, total_pages=num_pages,
                 user_id_for_stub=user_id, # Only user needed here technically
                 schema_id_for_stub=schema_id, # Pass schema_id to page task
             )
             logger.info(f"Job {job_id}: Scatter phase complete. Status set to PROCESSING.")
        else: # No pages enqueued (error)
             await update_job_status_in_redis(
                 ctx, job_id, JobStatus.FAILED, 
                 redis_client_override=redis_client, 
                 error_message="No pages found or processed.",
                 user_id_for_stub=user_id, document_id_for_stub=document_id,
                 file_id_for_stub=file_id, job_type_for_stub=job_type,
                 schema_id_for_stub=schema_id, # Pass schema_id to page task
             )


    except Exception as e:
        logger.error(f"Job {job_id}: Failed during page extraction/enqueue: {e}", exc_info=True)
        await update_job_status_in_redis(
            ctx, job_id, JobStatus.FAILED, 
            redis_client_override=redis_client, 
            error_message=f"Page Processing Error: {e}",
            user_id_for_stub=user_id, document_id_for_stub=document_id,
            file_id_for_stub=file_id, job_type_for_stub=job_type,
            schema_id_for_stub=schema_id, # Pass schema_id to page task
        )
        return

# --- ARQ Worker Settings (MODIFIED cron_jobs) --- 

# This class defines the functions available to the worker
class WorkerSettings:
    functions = [process_document,
        process_page,            # 
        assemble_job_results,    # 
        check_batch_statuses] 
    # Add cron job to run check_batch_statuses every 10 seconds
    # --- Use arq.cron helper --- (MODIFIED)
    cron_jobs = [
        cron(
            check_batch_statuses, 
            second=set(range(0, 60, 10)), # Run every 10 seconds (0, 10, 20, ... 50)
            run_at_startup=True
        )
    ]
    # Add other settings like `redis_settings`, `max_jobs`, etc. if needed 