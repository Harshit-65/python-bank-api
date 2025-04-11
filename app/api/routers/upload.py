import os
import time
import uuid
import mimetypes
import json
from typing import List, Annotated, Optional
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status, Request, Form, Query
from fastapi.security import OAuth2PasswordBearer # Import oauth2_scheme
from supabase.client import Client
from supabase.lib.client_options import ClientOptions
from supabase._sync.client import SyncStorageClient # Updated import path
# Import the helper function and specific exceptions, not AuthData type alias
from ...auth.dependencies import _perform_key_verification, oauth2_scheme, APIKeyNotFound, KeyfolioVerificationError, InvalidTokenError, RateLimitExceededError
from ...db.supabase_client import get_supabase_client
from ..models.requests import ALLOWED_EXTENSIONS, MAX_FILES, MAX_FILE_SIZE_BYTES, MAX_FILE_SIZE_MB
from ..models.responses import UploadResponseModel, UploadedFileModel
from ..utils.response_utils import create_success_response
# Import PyMuPDF (fitz) - make sure it's in requirements.txt
try:
    import fitz # PyMuPDF
except ImportError:
    fitz = None # Handle missing optional dependency

# Import Google AI for classification
try:
    import google.generativeai as genai
    from PIL import Image
    from io import BytesIO
except ImportError:
    genai = None
    Image = None
    BytesIO = None


router = APIRouter(
    prefix="/upload",
    tags=["Upload"],
    # REMOVE router-level dependency: dependencies=[Depends(AuthData)],
)

# --- File Validation Dependency --- # REMOVE AuthData here

# Simple validation function (can be expanded)
async def validate_file(file: UploadFile): # Make async as it reads the file
    """Basic validation for uploaded file."""
    if not file:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")

    # Validate file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file type for {file.filename}. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
        )

    # Validate file size (FastAPI/Starlette handle max size based on server config usually,
    # but we can add an explicit check if needed against our constant)
    # This requires reading the file, so it might be better done in the handler if large files are expected
    # if file.size > MAX_FILE_SIZE_BYTES:
    #     raise HTTPException(
    #         status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
    #         detail=f"File {file.filename} exceeds the maximum size of {MAX_FILE_SIZE_MB}MB."
    #     )

    # Validate PDF using PyMuPDF (if available)
    if fitz and file_ext == ".pdf":
        try:
            # Read file content into memory for validation
            file_content = await file.read() # Use await here as reading can be async
            await file.seek(0) # Reset file pointer after reading

            # Try opening the PDF
            pdf_doc = fitz.open(stream=file_content, filetype="pdf")
            if pdf_doc.page_count == 0:
                raise ValueError("PDF has no pages.")
            # Can add more checks here (e.g., is_encrypted)
            pdf_doc.close()
        except Exception as e:
            await file.seek(0) # Ensure pointer is reset even on error
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid or corrupt PDF file '{file.filename}': {e}"
            )

    # Reset pointer if not PDF or validation passed without reading
    await file.seek(0)
    return file

# --- Helper Functions ---

def get_storage_client(supabase: Client = Depends(get_supabase_client)) -> SyncStorageClient:
    """Dependency to get the Supabase storage client."""
    # Note: supabase-py v1.x storage client might be synchronous. Adjust if using v2+.
    # If async storage client is available and preferred, make this function async
    # and adjust usage in the endpoint.
    return supabase.storage

async def upload_file_to_storage(
    storage: SyncStorageClient, # Change to AsyncStorageClient if using async version
    file: UploadFile,
    user_id: str,
    doc_id: str, # Use document ID from DB as part of path
    doc_type: str = "invoice" # Add document type parameter with default
) -> tuple[str, str]:
    """Uploads a file to Supabase storage and returns (storage_path, public_url)."""
    content = await file.read()
    await file.seek(0) # Reset pointer in case it's needed again
    file_ext = os.path.splitext(file.filename)[1].lower()
    
    # Generate a unique filename using timestamp (similar to Go implementation)
    timestamp = int(time.time() * 1e9)
    unique_filename = f"{timestamp}_{file.filename}"
    
    # Use unique_filename for storage path
    storage_path = f"{user_id}/{unique_filename}"
    content_type = file.content_type or mimetypes.guess_type(file.filename)[0] or 'application/octet-stream'

    # Map file type to bucket (similar to Go code)
    bucket = "invoices"
    if doc_type == "bank_statement":
        bucket = "bank_statements"

    try:
        # Assuming sync storage client for now based on current supabase-py
        # Use `await storage.from_(bucket).upload(...)` if async client is used
        print(f"[Upload] Attempting to upload to storage path: {storage_path} in bucket: {bucket}") # DEBUG
        storage.from_(bucket).upload(
            path=storage_path,
            file=content,
            file_options={"content-type": content_type, "upsert": "true"} # upsert=true replaces if exists
        )
        print(f"[Upload] Successfully uploaded to storage path: {storage_path} in bucket: {bucket}") # DEBUG
        
        # Construct the public URL using Supabase URL from environment
        supabase_url = os.getenv("SUPABASE_URL")
        if not supabase_url:
            raise ValueError("SUPABASE_URL environment variable not set")
            
        public_url = f"{supabase_url}/storage/v1/object/public/{bucket}/{storage_path}"
        
        # Return both the storage path and the public URL
        return unique_filename, public_url
    except Exception as e:
        print(f"[Upload] Error uploading file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "UPLOAD_ERROR", "message": "Failed to upload file", "details": str(e)}
        )

# --- Document Classification ---

async def classify_document(file_content: bytes) -> str:
    """Classifies a document as 'bank_statement' or 'invoice' using Gemini."""
    if not genai or not Image or not BytesIO:
        print("[Upload] [CLASSIFY] Gemini or PIL not available, defaulting to 'invoice'")
        return "invoice"
    
    try:
        print("[Upload] [CLASSIFY] Starting document classification")
        
        # Configure Gemini
        from ...config import GOOGLE_API_KEY
        if not GOOGLE_API_KEY:
            print("[Upload] [CLASSIFY] No Google API key available, defaulting to 'invoice'")
            return "invoice"
        
        genai.configure(api_key=GOOGLE_API_KEY)
        
        # Create a temporary file for the PDF
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
            temp_file.write(file_content)
            temp_path = temp_file.name
        
        try:
            # Open the PDF with PyMuPDF
            pdf_doc = fitz.open(temp_path)
            if pdf_doc.page_count == 0:
                print("[Upload] [CLASSIFY] PDF has no pages, defaulting to 'invoice'")
                return "invoice"
            
            # Get the first page as an image
            page = pdf_doc.load_page(0)
            pix = page.get_pixmap()
            
            # Convert to PIL Image
            img_data = pix.tobytes("png")
            img = Image.open(BytesIO(img_data))
            
            # Prepare for Gemini
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            # Create the prompt
            prompt = "Analyze this document and determine if it's a bank statement or an invoice. Respond with ONLY one of these two words: 'bank_statement' or 'invoice'. Do not include any other text in your response."
            
            # Call Gemini
            response = await model.generate_content_async([prompt, img])
            
            # Extract the response text
            response_text = response.text.strip().lower()
            print(f"[Upload] [CLASSIFY] Raw Gemini response: {response_text}")
            
            # Determine document type
            if "bank_statement" in response_text or "bank statement" in response_text:
                doc_type = "bank_statement"
            elif "invoice" in response_text:
                doc_type = "invoice"
            else:
                print(f"[Upload] [CLASSIFY] Unexpected classification response: {response_text}, defaulting to 'invoice'")
                doc_type = "invoice"
            
            print(f"[Upload] [CLASSIFY] Document classified as: {doc_type}")
            return doc_type
            
        finally:
            # Clean up
            if 'pdf_doc' in locals():
                pdf_doc.close()
            os.unlink(temp_path)
            
    except Exception as e:
        print(f"[Upload] [CLASSIFY] Error during classification: {e}, defaulting to 'invoice'")
        return "invoice"


# --- Router ---

@router.post("",
             # response_model=UploadResponseModel, # Returning JSONResponse manually
             status_code=status.HTTP_200_OK,
             summary="Upload Files for Processing",
             description="Uploads one or more files (PDF, JPG, PNG). Files are saved to storage and a document record is created.")
async def handle_upload(
    # Inject dependencies needed for manual auth + upload
    request: Request,
    token: Annotated[str | None, Depends(oauth2_scheme)],
    supabase: Annotated[Client, Depends(get_supabase_client)],
    storage: Annotated[SyncStorageClient, Depends(get_storage_client)],
    files: Annotated[List[UploadFile], File(description=f"Files to upload (max {MAX_FILES}, types: {', '.join(ALLOWED_EXTENSIONS)})")] = [],
    doc_type: Annotated[Optional[str], Form()] = None,  # Fixed: Use = for default value
):
    """Handle file uploads for processing."""
    print(f"[Upload] Starting handle_upload with {len(files)} files, doc_type: {doc_type}")
    # --- Manual Authentication Step ---
    if token is None:
        print("[Upload] No token provided")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "MISSING_AUTH_HEADER", "message": "Missing Authorization header", "details": "Include 'Authorization: Bearer YOUR_API_KEY'"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    try:
        # Perform verification using the helper function
        print("[Upload] Performing API key verification...") # DEBUG
        auth_data = await _perform_key_verification(token, supabase)
        user_id = auth_data["user_id"]
        api_key_id = auth_data["api_key_id"]
        print(f"[Upload] Authentication successful for user_id: {user_id}, api_key_id: {api_key_id}") # DEBUG

        # Store in request state for logging middleware
        request.state.user_id = user_id
        request.state.api_key_id = api_key_id

    except (APIKeyNotFound, KeyfolioVerificationError, InvalidTokenError, RateLimitExceededError) as auth_exc:
        # If auth fails, re-raise the specific exception
        print(f"[Upload] Authentication failed: {auth_exc.detail}") # DEBUG
        raise auth_exc
    except Exception as exc:
        # Catch any other unexpected error during auth
        print(f"[Upload] Unexpected error during manual authentication: {exc}") # DEBUG
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error during authentication.")
    # --- End Manual Authentication ---

    # --- Proceed with Upload Logic (only if auth passed) ---
    if not files:
        print("[Upload] No files provided")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No files provided.")
    if len(files) > MAX_FILES:
        print(f"[Upload] Too many files: {len(files)} > {MAX_FILES}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Maximum {MAX_FILES} files allowed per upload.")

    uploaded_files_data: List[UploadedFileModel] = []
    overall_status = "done" # Assume success initially

    for file in files:
        doc_id = None
        file_url = None
        upload_error = None
        file_size = None
        determined_doc_type = None

        try:
            print(f"[Upload] Processing file: {file.filename}") # DEBUG
            # Get file size before validation
            temp_file_obj = file.file
            temp_file_obj.seek(0, os.SEEK_END)
            file_size = temp_file_obj.tell()
            await file.seek(0) # Reset pointer
            print(f"[Upload] File size: {file_size} bytes") # DEBUG

            if file_size > MAX_FILE_SIZE_BYTES:
                 print(f"[Upload] File too large: {file_size} > {MAX_FILE_SIZE_BYTES}")
                 raise HTTPException(
                     status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                     detail=f"File {file.filename} ({file_size / 1024 / 1024:.2f}MB) exceeds the maximum size of {MAX_FILE_SIZE_MB}MB."
                 )

            # 1. Validate individual file (type, potentially size, PDF validity)
            await validate_file(file) # Use await as validation reads file
            print(f"[Upload] File validation passed for: {file.filename}") # DEBUG

            # *** Determine document type (similar to Go implementation) ***
            file_ext = os.path.splitext(file.filename)[1].lower()
            
            # Use provided doc_type if available
            if doc_type:
                print(f"[Upload] Using provided document type: {doc_type}")
                determined_doc_type = doc_type
            # For PDFs, try to classify with Gemini
            elif file_ext == ".pdf":
                print(f"[Upload] PDF detected, attempting classification with Gemini")
                # Read file content for classification
                file_content = await file.read()
                await file.seek(0)  # Reset pointer
                
                # Classify the document
                determined_doc_type = await classify_document(file_content)
                print(f"[Upload] Classification result: {determined_doc_type}")
            # For non-PDFs, default to invoice
            else:
                print(f"[Upload] Non-PDF file, defaulting to 'invoice'")
                determined_doc_type = "invoice"
            
            print(f"[Upload] Final determined doc_type: {determined_doc_type}") # DEBUG

            # 2. Create initial document record in DB
            db_record = {
                "user_id": user_id,
                "file_name": file.filename,
                "file_size": file_size,
                "document_type": determined_doc_type, # Use the determined type
                "status": "pending", # Changed from "uploaded" to "pending" to match DB constraint
            }
            print(f"[Upload] Inserting document record into DB: {db_record}") # DEBUG
            try:
                # Remove await for sync Supabase client
                insert_response = supabase.table("documents").insert(db_record, returning="representation").execute()
                print(f"[Upload] DB insert response: {insert_response}") # DEBUG

                if not insert_response.data or not insert_response.data[0].get('id'):
                     # Log the actual response for debugging
                     print(f"[Upload] Failed DB insert response for {file.filename}: {insert_response}")
                     raise HTTPException(status_code=500, detail=f"Failed to create database record for {file.filename} or retrieve ID")

                doc_id = insert_response.data[0]['id']
                print(f"[Upload] DB record created with doc_id: {doc_id}") # DEBUG
            except Exception as db_error:
                print(f"[Upload] Database error: {db_error}")
                # Try to get more details about the error
                if hasattr(db_error, 'response'):
                    print(f"[Upload] Error response: {db_error.response}")
                raise HTTPException(status_code=500, detail=f"Database error: {str(db_error)}")

            # 3. Upload to Supabase Storage
            unique_filename, file_url = await upload_file_to_storage(storage, file, user_id, doc_id, determined_doc_type)
            print(f"[Upload] File uploaded to storage path: {unique_filename} with URL: {file_url}")

            # Optionally update DB record with storage_path if needed
            # supabase.table("documents").update({"storage_path": storage_path}).eq("id", doc_id).execute()

        except HTTPException as e:
            # Handle validation/upload errors
            upload_error = f"Code: {e.status_code}, Detail: {e.detail}"
            overall_status = "partial_error"
            print(f"[Upload] HTTPException for {file.filename}: {upload_error}") # DEBUG
            # If DB record was created but upload failed, potentially update status to 'failed'
            if doc_id:
                 try:
                    supabase.table("documents").update({"status": "failed"}).eq("id", doc_id).execute()
                    print(f"[Upload] Updated document status to 'failed' for doc_id: {doc_id}")
                 except Exception as db_update_err:
                     print(f"[Upload] Failed to update status for failed upload doc {doc_id}: {db_update_err}")

        except Exception as e:
            # Catch unexpected errors
            upload_error = f"Unexpected error processing {file.filename}: {str(e)}"
            overall_status = "partial_error"
            print(f"[Upload] Unexpected Exception for {file.filename}: {upload_error}") # DEBUG
            if doc_id:
                 try:
                    supabase.table("documents").update({"status": "failed"}).eq("id", doc_id).execute()
                    print(f"[Upload] Updated document status to 'failed' for doc_id: {doc_id}")
                 except Exception as db_update_err:
                     print(f"[Upload] Failed to update status for failed upload doc {doc_id}: {db_update_err}")

        finally:
            # Ensure file resources are closed
             await file.close()
             print(f"[Upload] Closed file: {file.filename}")


        # --- Append Result Inside Loop --- (MOVED)
        uploaded_files_data.append(
             UploadedFileModel(
                 file_id=unique_filename if not upload_error and unique_filename else file.filename, # Use unique ID on success, else original name
                 doc_id=doc_id, 
                 file_url=file_url, 
                 error=upload_error,
                 type=determined_doc_type 
             )
         )
        print(f"[Upload] Appended result for: {file.filename}") # DEBUG
        # --- End Append Result --- 

    # Return standardized success response
    response_data = UploadResponseModel(files=uploaded_files_data, status=overall_status)
    status_code = status.HTTP_207_MULTI_STATUS if overall_status == "partial_error" else status.HTTP_200_OK
    print(f"[Upload] Finished processing all files. Overall status: {overall_status}. Returning status code: {status_code}") # DEBUG

    return create_success_response(data=response_data.model_dump(exclude_none=True), status_code=status_code)