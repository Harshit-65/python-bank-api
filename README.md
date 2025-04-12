# Python Bank API

This project is a Python FastAPI implementation of the Bank Statement and Invoice Parsing API, designed to mirror the functionality of its Go counterpart. It provides endpoints for uploading documents, submitting parsing jobs (individually or in batches), checking job status, and retrieving results. It utilizes Supabase for database/storage, Redis for caching/queuing, ARQ for background task processing, and Google Gemini for AI-driven parsing.

## Prerequisites

*   Python 3.9+
*   `pip` (Python package installer)

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Harshit-65/python-bank-api.git
    cd python-bank-api
    ```

2.  **Create and activate a virtual environment:**
    *   On macOS/Linux:
        ```bash
        python3 -m venv venv
        source venv/bin/activate
        ```
    *   On Windows:
        ```bash
        python -m venv venv
        .\\venv\\Scripts\\activate
        ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Environment Variables

Create a `.env` file in the `python-bank-api` root directory and populate it with the necessary credentials and configurations. See the `.env.example` file (if provided) or the list below:

```env
# Supabase Credentials (Required)
SUPABASE_URL=https://<your_project_ref>.supabase.co
SUPABASE_KEY=<your_supabase_service_role_key>

# Redis Configuration (Required)
REDIS_URL=redis://[:password@]host:port[/db] # e.g., redis://localhost:6379 or redis://:<password>@hostname:port
REDIS_PASSWORD=<your_redis_password> # Required if your Redis instance uses a password

# Google AI API Key (Required for parsing)
# Provide at least one key. Add GOOGLE_API_KEY2, GOOGLE_API_KEY3, etc., for load balancing.
GOOGLE_API_KEY=<your_google_ai_api_key>
# GOOGLE_API_KEY2=...

# Keyfolio Configuration (Required for Authentication)
KEYFOLIO_URL=https://keyfolio-febpthnnhq-ue.a.run.app # Or your Keyfolio instance URL
# KEYFOLIO_ADMIN_KEY=... # Needed for managing keys, not for running the API itself
```



## Running the API

Use `uvicorn` to run the FastAPI application:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

*   `--reload`: Enables auto-reloading when code changes (useful for development).
*   `--host 0.0.0.0`: Makes the API accessible from other machines on your network.
*   `--port 8000`: Specifies the port to run on.



## Running the Background Worker

The background worker processes the parsing jobs queued by the API. Run it in a separate terminal:

```bash
python worker.py
```

The worker needs access to the same environment variables as the API. It connects to Redis to pick up jobs defined in `app/queue/tasks.py`. The `worker.py` script includes basic restart logic for resilience against certain errors (like Redis timeouts).

## Authentication

Most API endpoints require authentication via an API key. Include the key in the `Authorization` header as a Bearer token:

```
Authorization: Bearer <your_api_key>
```

The API key is verified against the Keyfolio service and linked to a `user_id` via the Supabase `api_keys` table.

## Available API Endpoints

The base path for all endpoints is `/api/v1`.

### Health Check

*   **`GET /health`**: Checks the status of the API and its connection to the database. (No Auth Required)

### File Upload

*   **`POST /parse/upload`**: Uploads one or more files (PDF, JPG, PNG).
    *   Performs validation (type, size, PDF integrity).
    *   Classifies PDFs using Gemini (invoice/bank\_statement).
    *   Creates a `documents` record in Supabase.
    *   Uploads the file to the appropriate Supabase Storage bucket (`invoices` or `bank_statements`).
    *   Returns `file_id` (used for storage path), `doc_id` (database record ID), `file_url`, and determined `type` for each file.
    *   Requires `Authorization` header.
    *   Accepts `multipart/form-data`. Use `files` for the file uploads.

### Bank Statement Parsing

*   **`POST /parse/statements`**: Submits an uploaded bank statement for parsing.
    *   Requires `Authorization` header.
    *   Request Body: `ParseRequestModel` (JSON)
        ```json
        {
          "file_id": "timestamp_filename.pdf", // The file_id returned by upload
          "document_id": "uuid-of-document-record", // The doc_id returned by upload
          "callback_url": "optional-url-for-notification"
        }
        ```
    *   Enqueues a background job (`process_document` task).
    *   Returns a `JobCreatedResponseModel` with `job_id` and `status: pending`.

*   **`GET /parse/statements/{job_id}`**: Retrieves the status and results of a specific bank statement parsing job.
    *   Requires `Authorization` header.
    *   Path Parameter: `job_id`.
    *   Fetches job data from Redis.
    *   Returns a `JobResponseModel` including status, results (if completed), error message (if failed), timestamps.

*   **`GET /parse/statements`**: Lists bank statement parsing jobs for the user.
    *   Requires `Authorization` header.
    *   Query Parameters: `page` (int, default 1), `page_size` (int, default 20), `status` (optional: pending, processing, completed, failed).
    *   Fetches job data from Redis.
    *   Returns a `PaginatedJobResponse`.

*   **`POST /parse/statements/batch`**: Submits a batch of uploaded bank statements for parsing.
    *   Requires `Authorization` header.
    *   Request Body: `BatchRequestModel` (JSON)
        ```json
        {
          "files": [
            { "file_id": "file1.pdf", "document_id": "doc_id_1"},
            { "file_id": "file2.pdf", "document_id": "doc_id_2" }
            // ... up to 50 files
          ],
          "callback_url": "optional-url-for-batch-notification"
        }
        ```
    *   Enqueues multiple individual jobs.
    *   Creates batch metadata in Redis.
    *   Returns a `BatchCreatedResponseModel` with `batch_id`.

*   **`GET /parse/statements/batch/{batch_id}`**: Retrieves the status of a batch job and the status of its constituent files.
    *   Requires `Authorization` header.
    *   Path Parameter: `batch_id`.
    *   Fetches batch and job data from Redis.
    *   Returns a `BatchStatusResponseModel`.

### Invoice Parsing

*   **`POST /parse/invoices`**: Submits an uploaded invoice for parsing.
    *   Requires `Authorization` header.
    *   Request Body: `ParseRequestModel` (JSON, similar to statements).
    *   Enqueues a background job.
    *   Returns `job_id`.

*   **`GET /parse/invoices/{job_id}`**: Retrieves the status and results of a specific invoice parsing job.
    *   Requires `Authorization` header.
    *   Path Parameter: `job_id`.
    *   Fetches job data from Redis.
    *   Returns `JobResponseModel`.

*   **`GET /parse/invoices`**: Lists invoice parsing jobs for the user.
    *   Requires `Authorization` header.
    *   Query Parameters: `page`, `page_size`, `status`.
    *   Fetches job data from Redis.
    *   Returns `PaginatedJobResponse`.

*   **`POST /parse/invoices/batch`**: Submits a batch of uploaded invoices for parsing.
    *   Requires `Authorization` header.
    *   Request Body: `BatchRequestModel` (JSON, similar to statements).
    *   Enqueues multiple individual jobs.
    *   Creates batch metadata in Redis.
    *   Returns `batch_id`.

*   **`GET /parse/invoices/batch/{batch_id}`**: Retrieves the status of an invoice batch job.
    *   Requires `Authorization` header.
    *   Path Parameter: `batch_id`.
    *   Fetches batch and job data from Redis.
    *   Returns `BatchStatusResponseModel`.



## Important Notes

*   **Dependencies:** This API relies heavily on external services:
    *   **Supabase:** For user authentication linkage, document metadata storage, and file storage.
    *   **Keyfolio:** For API key validation and rate limiting.
    *   **Redis:** For background job queuing (ARQ) and caching job/batch status.
    *   **Google Gemini API:** For the actual document parsing. Ensure you have valid API keys with billing enabled.
*   **Background Worker:** The `worker.py` process *must* be running for any submitted parsing jobs to be processed.
*   **Concurrency Model:** The worker uses a scatter-gather approach. `process_document` downloads the file and enqueues `process_page` tasks for each page. `process_page` calls Gemini. `assemble_job_results` gathers page results and updates the final job status.
*   **Error Handling:** Check API responses for success/error status and details. Logs provide more in-depth information (API logs in `api_logs` table in Supabase, worker logs in the console where `worker.py` is running). 