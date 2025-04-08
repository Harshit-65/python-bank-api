import logging
import os
import json
import random
from io import BytesIO
from typing import Dict, Any, Optional
import mimetypes
from datetime import datetime

import google.generativeai as genai
from supabase import create_client, Client
from PIL import Image
import fitz # PyMuPDF

from ..config import SUPABASE_URL, SUPABASE_KEY, GOOGLE_API_KEY, GOOGLE_API_KEYS
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

async def update_job_status(supabase: Client, job_id: str, status: JobStatus, results: Optional[Dict[str, Any]] = None, error_message: Optional[str] = None):
    """Updates job status and optionally results/error in Supabase."""
    update_data = {"status": status.value, "updated_at": datetime.now().isoformat()}
    if results is not None:
        update_data["results"] = json.dumps(results) # Store results as JSON string
    if error_message:
        # Add an error field to your jobs table schema if needed
        update_data["error_message"] = error_message 
    try:
        await supabase.table("jobs").update(update_data).eq("id", job_id).execute()
        logger.info(f"Updated job {job_id} status to {status.value}")
    except Exception as e:
        logger.error(f"Failed to update job {job_id} status: {e}")

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

async def process_document(ctx: dict, job_id: str, document_id: str):
    """ARQ task to process a single document (statement or invoice)."""
    supabase = get_sync_supabase_client() # Use sync client in worker task
    logger.info(f"Starting processing for job_id: {job_id}, document_id: {document_id}")

    # 1. Fetch Job and Document Details
    try:
        job_response = await supabase.table("jobs").select("*, documents(*)").eq("id", job_id).single().execute()
        job_data = job_response.data
        doc_data = job_data.get("documents")

        if not doc_data:
            raise ValueError("Document data not found for job")
            
        await update_job_status(supabase, job_id, JobStatus.PROCESSING)

        user_id = doc_data['user_id']
        file_name = doc_data['file_name']
        job_type = job_data['job_type'] # 'bank_statement' or 'invoice'
        
        # Construct storage path (mirroring upload logic)
        file_ext = os.path.splitext(file_name)[1].lower()
        storage_path = f"{user_id}/{document_id}{file_ext}"
        
        # Map file type to bucket (similar to upload.py)
        bucket_name = "invoices"
        if job_type == "bank_statement":
            bucket_name = "bank_statements"

    except Exception as e:
        logger.error(f"Job {job_id}: Failed to fetch job/document details: {e}")
        await update_job_status(supabase, job_id, JobStatus.FAILED, error_message=f"DB Error: {e}")
        return

    # 2. Download File from Storage
    try:
        logger.info(f"Job {job_id}: Downloading {storage_path} from bucket {bucket_name}")
        # Note: supabase-py storage client is sync
        file_bytes = supabase.storage.from_(bucket_name).download(storage_path)
        if not file_bytes:
            raise FileNotFoundError("Downloaded file is empty")
        logger.info(f"Job {job_id}: Downloaded {len(file_bytes)} bytes.")
    except Exception as e:
        logger.error(f"Job {job_id}: Failed to download file {storage_path}: {e}")
        await update_job_status(supabase, job_id, JobStatus.FAILED, error_message=f"Storage Error: {e}")
        return

    # 3. Extract Content (PDF/Image)
    content_parts = []
    is_pdf = file_ext == ".pdf"
    prompt = BANK_STATEMENT_PROMPT if job_type == 'bank_statement' else INVOICE_PROMPT

    try:
        if is_pdf:
            pdf_doc = fitz.open(stream=file_bytes, filetype="pdf")
            logger.info(f"Job {job_id}: Processing {pdf_doc.page_count} pages.")
            for page_num in range(pdf_doc.page_count):
                page = pdf_doc.load_page(page_num)
                # Option 1: Extract text (might be less accurate for complex layouts)
                # text = page.get_text()
                # if text.strip():
                #    content_parts.append(text)
                
                # Option 2: Extract image (better for Gemini vision)
                pix = page.get_pixmap()
                img_bytes = pix.tobytes("png") # Convert to PNG bytes
                content_parts.append({"mime_type": "image/png", "data": img_bytes})
            pdf_doc.close()
        else: # Handle Images
            img = Image.open(BytesIO(file_bytes))
            # Convert to PNG or JPEG bytes for Gemini
            output_buffer = BytesIO()
            img_format = 'png' # Or jpeg
            img.save(output_buffer, format=img_format)
            content_parts.append({"mime_type": f"image/{img_format}", "data": output_buffer.getvalue()})
            logger.info(f"Job {job_id}: Processing image.")
            
        if not content_parts:
            raise ValueError("No content extracted from document")

    except Exception as e:
        logger.error(f"Job {job_id}: Failed to extract content: {e}")
        await update_job_status(supabase, job_id, JobStatus.FAILED, error_message=f"Content Extraction Error: {e}")
        return

    # 4. Call Google AI (Gemini)
    try:
        logger.info(f"Job {job_id}: Calling Gemini API...")
        ai_response_text = await call_gemini(prompt, content_parts)
        logger.info(f"Job {job_id}: Received response from Gemini.")
        # logger.debug(f"Job {job_id}: Gemini Raw Response: {ai_response_text}")

    except Exception as e:
        logger.error(f"Job {job_id}: Gemini API call failed: {e}")
        await update_job_status(supabase, job_id, JobStatus.FAILED, error_message=f"AI Error: {e}")
        return

    # 5. Parse AI Response
    parsed_results = extract_json_from_text(ai_response_text)

    if parsed_results is None:
        logger.error(f"Job {job_id}: Failed to parse JSON from Gemini response.")
        await update_job_status(supabase, job_id, JobStatus.FAILED, error_message="AI Response Parsing Error")
        # Optionally save raw response: await update_job_status(..., results={"raw_response": ai_response_text})
        return
        
    # Basic validation/cleaning (optional)
    # e.g., ensure confidence_score exists
    if 'confidence_score' not in parsed_results:
        parsed_results['confidence_score'] = 0.0 # Assign default if missing
        logger.warning(f"Job {job_id}: Confidence score missing from AI response, set to 0.0")

    logger.info(f"Job {job_id}: Successfully parsed AI response.")

    # 6. Update Job Status and Results
    await update_job_status(supabase, job_id, JobStatus.COMPLETED, results=parsed_results)

    logger.info(f"Finished processing job_id: {job_id}")

# --- ARQ Worker Settings --- 

# This class defines the functions available to the worker
class WorkerSettings:
    functions = [process_document]
    # Add other settings like `redis_settings`, `max_jobs`, etc. if needed
    # redis_settings = arq_redis_settings # Use settings from arq_client 