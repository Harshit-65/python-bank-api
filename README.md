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

This project requires several environment variables to be set for accessing external services like Supabase, Redis, Google AI, and Keyfolio.

1.  **Copy the Example File:** Create a copy of the `.env.example` file in the project root directory and name it `.env`.
    *   On macOS/Linux:
        ```bash
        cp .env.example .env
        ```
    *   On Windows (Command Prompt):
        ```bash
        copy .env.example .env
        ```
    *   On Windows (PowerShell):
        ```bash
        Copy-Item .env.example .env
        ```

2.  **Edit `.env**: Open the newly created `.env` file in a text editor.

3.  **Fill in Values:** Replace the placeholder values (like `<your_value>`, example URLs, etc.) with your actual credentials and configuration details for each required service.

4.  **Save the File:** Ensure the file is saved.

The application automatically loads these variables when it starts.

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

## API Endpoint Guide

This section provides details on how to use each endpoint, including examples for testing with tools like Postman.

**Base URL:** `http://localhost:8000/api/v1` (assuming the API is running locally on port 8000)

---

### 1. Health Check

Checks the operational status of the API and its dependencies.

*   **Endpoint:** `GET /health`
*   **Full URL:** `http://localhost:8000/api/v1/health`
*   **Description:** Verifies connectivity to the database and returns the overall service status.
*   **Authentication:** None Required

*   **Example Success Response (200 OK):**
    ```json
    {
      "success": true,
      "data": {
        "status": "ok",
        "timestamp": "2025-04-15T10:30:00.123456Z",
        "services": {
          "database": {
            "status": true,
            "message": null
          }
        }
      }
    }
    ```
*   **Postman Setup:**
    *   Method: `GET`
    *   URL: `http://localhost:8000/api/v1/health`
    *   Auth: `No Auth`
    *   Body: `none`

---

### 2. Schema Management

Endpoints for creating, listing, retrieving, updating, and deleting custom parsing schemas.

#### 2.1 Create Schema

*   **Endpoint:** `POST /schemas`
*   **Full URL:** `http://localhost:8000/api/v1/schemas`
*   **Description:** Creates a new custom schema (e.g., for invoices or bank statements).
*   **Authentication:** Required (`Bearer Token`)

*   **Example Request Body (JSON):**
    ```json
    {
      "name": "My Custom Invoice Schema v1",
      "type": "invoice",
      "description": "Schema for standard vendor invoices.",
      "schema_string": "{\\\"invoice_number\\\": \\\"\\\", \\\"invoice_date\\\": \\\"\\\", \\\"total_amount\\\": 0.0}"
    }
    ```
    *Note: The `schema_string` must be a valid JSON string itself.*

*   **Example Success Response (201 Created):**
    ```json
    {
      "success": true,
      "data": {
        "id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
        "name": "My Custom Invoice Schema v1",
        "type": "invoice",
        "description": "Schema for standard vendor invoices.",
        "version": 1,
        "created_at": "2025-04-15T10:35:00.987654Z",
        "updated_at": "2025-04-15T10:35:00.987654Z"
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `POST`
    *   URL: `http://localhost:8000/api/v1/schemas`
    *   Auth: `Bearer Token` (Paste your API key)
    *   Body: `raw`, `JSON`
    *   Body Content: (Paste the Example Request Body JSON)

#### 2.2 List Schemas

*   **Endpoint:** `GET /schemas/list`
*   **Full URL:** `http://localhost:8000/api/v1/schemas/list`
*   **Description:** Lists active schemas for the user, with pagination and optional type filtering.
*   **Authentication:** Required (`Bearer Token`)
*   **Query Parameters:**
    *   `page` (integer, optional, default: 1)
    *   `page_size` (integer, optional, default: 20)
    *   `type` (string, optional, values: `invoice` or `bank_statement`)
*   **Example URL with Query Params:** `http://localhost:8000/api/v1/schemas/list?page=1&page_size=10&type=invoice`

*   **Example Success Response (200 OK):**
    ```json
    {
      "success": true,
      "data": {
        "schemas": [
          {
            "id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
            "name": "My Custom Invoice Schema v1",
            "type": "invoice",
            "description": "Schema for standard vendor invoices.",
            "version": 1,
            "created_at": "2025-04-15T10:35:00.987654Z",
            "updated_at": "2025-04-15T10:35:00.987654Z"
          }
          // ... more schemas ...
        ],
        "total": 1,
        "page": 1,
        "page_size": 10
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `GET`
    *   URL: `http://localhost:8000/api/v1/schemas/list`
    *   Auth: `Bearer Token`
    *   Params: (Add query parameters like `page`, `page_size`, `type` as needed)
    *   Body: `none`

#### 2.3 Get Schema Details

*   **Endpoint:** `GET /schemas/id/{schema_id}`
*   **Full URL Example:** `http://localhost:8000/api/v1/schemas/id/a1b2c3d4-e5f6-7890-1234-567890abcdef`
*   **Description:** Retrieves full details of a specific schema, including the `schema_string`.
*   **Authentication:** Required (`Bearer Token`)
*   **Path Parameter:** `schema_id` (UUID of the schema)

*   **Example Success Response (200 OK):**
    ```json
    {
      "success": true,
      "data": {
        "id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
        "name": "My Custom Invoice Schema v1",
        "type": "invoice",
        "description": "Schema for standard vendor invoices.",
        "version": 1,
        "created_at": "2025-04-15T10:35:00.987654Z",
        "updated_at": "2025-04-15T10:35:00.987654Z",
        "schema_string": "{\\\"invoice_number\\\": \\\"\\\", \\\"invoice_date\\\": \\\"\\\", \\\"total_amount\\\": 0.0}"
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `GET`
    *   URL: `http://localhost:8000/api/v1/schemas/id/<YOUR_SCHEMA_ID>` (Replace `<YOUR_SCHEMA_ID>`)
    *   Auth: `Bearer Token`
    *   Body: `none`

#### 2.4 Update Schema

*   **Endpoint:** `PUT /schemas/id/{schema_id}`
*   **Full URL Example:** `http://localhost:8000/api/v1/schemas/id/a1b2c3d4-e5f6-7890-1234-567890abcdef`
*   **Description:** Updates the `description` and/or `schema_string` of an existing schema. Increments the version number automatically.
*   **Authentication:** Required (`Bearer Token`)
*   **Path Parameter:** `schema_id` (UUID of the schema)

*   **Example Request Body (JSON):**
    ```json
    {
      "description": "Updated description for standard vendor invoices.",
      "schema_string": "{\\\"invoice_number\\\": \\\"\\\", \\\"invoice_date\\\": \\\"\\\", \\\"total_amount\\\": 0.0, \\\"vendor_name\\\": \\\"\\\"}"
    }
    ```

*   **Example Success Response (200 OK):**
    ```json
    {
      "success": true,
      "data": {
        "id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
        "name": "My Custom Invoice Schema v1", // Name doesn't change
        "type": "invoice", // Type doesn't change
        "description": "Updated description for standard vendor invoices.",
        "version": 2, // Version incremented
        "created_at": "2025-04-15T10:35:00.987654Z",
        "updated_at": "2025-04-15T10:40:00.112233Z", // Updated timestamp
        "schema_string": "{\\\"invoice_number\\\": \\\"\\\", \\\"invoice_date\\\": \\\"\\\", \\\"total_amount\\\": 0.0, \\\"vendor_name\\\": \\\"\\\"}"
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `PUT`
    *   URL: `http://localhost:8000/api/v1/schemas/id/<YOUR_SCHEMA_ID>` (Replace `<YOUR_SCHEMA_ID>`)
    *   Auth: `Bearer Token`
    *   Body: `raw`, `JSON`
    *   Body Content: (Paste the Example Request Body JSON)

#### 2.5 Delete Schema

*   **Endpoint:** `DELETE /schemas/id/{schema_id}`
*   **Full URL Example:** `http://localhost:8000/api/v1/schemas/id/a1b2c3d4-e5f6-7890-1234-567890abcdef`
*   **Description:** Soft-deletes a schema (marks it as inactive).
*   **Authentication:** Required (`Bearer Token`)
*   **Path Parameter:** `schema_id` (UUID of the schema)

*   **Example Success Response:** `204 No Content` (No response body)

*   **Postman Setup:**
    *   Method: `DELETE`
    *   URL: `http://localhost:8000/api/v1/schemas/id/<YOUR_SCHEMA_ID>` (Replace `<YOUR_SCHEMA_ID>`)
    *   Auth: `Bearer Token`
    *   Body: `none`

---

### 3. File Upload

Uploads files for later processing.

*   **Endpoint:** `POST /parse/upload`
*   **Full URL:** `http://localhost:8000/api/v1/parse/upload`
*   **Description:** Uploads one or more files (PDF, JPG, PNG), classifies them, saves them to storage, and creates document records.
*   **Authentication:** Required (`Bearer Token`)
*   **Request Body Type:** `multipart/form-data`

*   **Example Success Response (200 OK or 207 Multi-Status if partial errors):**
    ```json
    {
      "success": true,
      "data": {
        "files": [
          {
            "file_id": "1713177600123456789_invoice.pdf", // Timestamped unique filename
            "file_url": "http://localhost:8000/storage/v1/object/public/invoices/user-id-123/1713177600123456789_invoice.pdf", // Example URL format
            "doc_id": "b1c2d3e4-f5g6-7890-1234-567890abcdef",
            "error": null,
            "type": "invoice"
          },
          {
            "file_id": "1713177605987654321_statement.png",
            "file_url": "http://localhost:8000/storage/v1/object/public/bank_statements/user-id-123/1713177605987654321_statement.png",
            "doc_id": "c2d3e4f5-g6h7-8901-2345-678901bcdefg",
            "error": null,
            "type": "bank_statement"
          }
        ],
        "status": "done" // or "partial_error"
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `POST`
    *   URL: `http://localhost:8000/api/v1/parse/upload`
    *   Auth: `Bearer Token`
    *   Body: `form-data`
    *   Body Content:
        *   Add one or more keys named `files`.
        *   For each `files` key, change the type from `Text` to `File`.
        *   Select the file(s) you want to upload from your computer.

---

### 4. Bank Statement Parsing

Endpoints specific to submitting and managing bank statement parsing jobs.

#### 4.1 Submit Bank Statement Job

*   **Endpoint:** `POST /parse/statements`
*   **Full URL:** `http://localhost:8000/api/v1/parse/statements`
*   **Description:** Submits a previously uploaded bank statement for asynchronous parsing. Can optionally specify a custom schema.
*   **Authentication:** Required (`Bearer Token`)

*   **Example Request Body (JSON):**
    ```json
    {
      "file_id": "1713177605987654321_statement.png", // file_id from upload response
      "document_id": "c2d3e4f5-g6h7-8901-2345-678901bcdefg", // doc_id from upload response
      "schema_id": null, // Or "uuid-of-custom-bank-statement-schema"
      "callback_url": "https://your-webhook-listener.com/callback" // Optional
    }
    ```

*   **Example Success Response (202 Accepted):**
    ```json
    {
      "success": true,
      "data": {
        "job_id": "job_d1e2f3a4-b5c6-7890-1234-567890abcdef",
        "status": "pending",
        "created_at": "2025-04-15T10:50:00.123123Z",
        "updated_at": "2025-04-15T10:50:00.123123Z"
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `POST`
    *   URL: `http://localhost:8000/api/v1/parse/statements`
    *   Auth: `Bearer Token`
    *   Body: `raw`, `JSON`
    *   Body Content: (Paste the Example Request Body JSON, using IDs from your upload response)

#### 4.2 Get Bank Statement Job Status

*   **Endpoint:** `GET /parse/statements/{job_id}`
*   **Full URL Example:** `http://localhost:8000/api/v1/parse/statements/job_d1e2f3a4-b5c6-7890-1234-567890abcdef`
*   **Description:** Retrieves the status and results (if completed/failed) of a specific bank statement job.
*   **Authentication:** Required (`Bearer Token`)
*   **Path Parameter:** `job_id` (from the submit response)

*   **Example Success Response (200 OK - Completed):**
    ```json
    {
      "success": true,
      "data": {
        "job_id": "job_d1e2f3a4-b5c6-7890-1234-567890abcdef",
        "status": "completed",
        "created_at": "2025-04-15T10:50:00.123123Z",
        "updated_at": "2025-04-15T10:55:00.456456Z",
        "result": {
          // ... Parsed bank statement data ...
          "all_transactions": [ ... ],
          "average_confidence_score": 0.85,
          "total_pages": 1
        },
        "error": null
      }
    }
    ```
*   **Example Success Response (200 OK - Processing):**
    ```json
    {
      "success": true,
      "data": {
        "job_id": "job_d1e2f3a4-b5c6-7890-1234-567890abcdef",
        "status": "processing",
        "created_at": "2025-04-15T10:50:00.123123Z",
        "updated_at": "2025-04-15T10:52:00.789789Z",
        "result": null,
        "error": null
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `GET`
    *   URL: `http://localhost:8000/api/v1/parse/statements/<YOUR_JOB_ID>` (Replace `<YOUR_JOB_ID>`)
    *   Auth: `Bearer Token`
    *   Body: `none`

#### 4.3 List Bank Statement Jobs

*   **Endpoint:** `GET /parse/statements`
*   **Full URL:** `http://localhost:8000/api/v1/parse/statements`
*   **Description:** Lists bank statement jobs for the user, with pagination and filtering.
*   **Authentication:** Required (`Bearer Token`)
*   **Query Parameters:**
    *   `page` (integer, optional, default: 1)
    *   `page_size` (integer, optional, default: 20)
    *   `status` (string, optional, values: `pending`, `processing`, `completed`, `failed`)
*   **Example URL:** `http://localhost:8000/api/v1/parse/statements?page_size=5&status=completed`

*   **Example Success Response (200 OK):**
    ```json
    {
      "success": true,
      "data": {
        "jobs": [
          {
            "job_id": "job_d1e2f3a4-b5c6-7890-1234-567890abcdef",
            "status": "completed",
            "created_at": "2025-04-15T10:50:00.123123Z",
            "updated_at": "2025-04-15T10:55:00.456456Z",
            "error_message": null,
            "file_id": "1713177605987654321_statement.png"
          }
          // ... more jobs ...
        ],
        "total": 1,
        "page": 1,
        "page_size": 5,
        "total_pages": 1
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `GET`
    *   URL: `http://localhost:8000/api/v1/parse/statements`
    *   Auth: `Bearer Token`
    *   Params: (Add query parameters like `page_size`, `status` as needed)
    *   Body: `none`

#### 4.4 Submit Bank Statement Batch Job

*   **Endpoint:** `POST /parse/statements/batch`
*   **Full URL:** `http://localhost:8000/api/v1/parse/statements/batch`
*   **Description:** Submits multiple bank statements for batch processing.
*   **Authentication:** Required (`Bearer Token`)

*   **Example Request Body (JSON):**
    ```json
    {
      "files": [
        {
          "file_id": "1713177605987654321_statement.png",
          "document_id": "c2d3e4f5-g6h7-8901-2345-678901bcdefg",
          "schema_id": null // Optional schema per file
        },
        {
          "file_id": "1713177700112233445_other_statement.pdf",
          "document_id": "d3e4f5g6-h7i8-9012-3456-789012cdefgh",
          "schema_id": null
        }
      ],
      "callback_url": "https://your-webhook-listener.com/batch_callback" // Optional
    }
    ```

*   **Example Success Response (202 Accepted):**
    ```json
    {
      "success": true,
      "data": {
        "batch_id": "batch_e1f2a3b4-c5d6-7890-1234-567890abcdef",
        "status": "pending",
        "total_files": 2,
        "created_at": "2025-04-15T11:00:00.555555Z",
        "callback_url": "https://your-webhook-listener.com/batch_callback"
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `POST`
    *   URL: `http://localhost:8000/api/v1/parse/statements/batch`
    *   Auth: `Bearer Token`
    *   Body: `raw`, `JSON`
    *   Body Content: (Paste the Example Request Body JSON, using IDs from your upload responses)

#### 4.5 Get Bank Statement Batch Job Status

*   **Endpoint:** `GET /parse/statements/batch/{batch_id}`
*   **Full URL Example:** `http://localhost:8000/api/v1/parse/statements/batch/batch_e1f2a3b4-c5d6-7890-1234-567890abcdef`
*   **Description:** Retrieves the status of a batch job and its individual file processing statuses.
*   **Authentication:** Required (`Bearer Token`)
*   **Path Parameter:** `batch_id` (from the batch submit response)

*   **Example Success Response (200 OK):**
    ```json
    {
      "success": true,
      "data": {
        "batch_id": "batch_e1f2a3b4-c5d6-7890-1234-567890abcdef",
        "status": "completed", // or processing, failed
        "total_files": 2,
        "processed_files": 2, // Files completed successfully
        "failed_files": 0,
        "created_at": "2025-04-15T11:00:00.555555Z",
        "updated_at": "2025-04-15T11:05:00.666666Z",
        "files": [
          {
            "file_id": "1713177605987654321_statement.png",
            "document_id": "c2d3e4f5-g6h7-8901-2345-678901bcdefg",
            "status": "completed",
            "error": null
          },
          {
            "file_id": "1713177700112233445_other_statement.pdf",
            "document_id": "d3e4f5g6-h7i8-9012-3456-789012cdefgh",
            "status": "completed",
            "error": null
          }
        ],
        "callback_url": "https://your-webhook-listener.com/batch_callback"
      }
    }
    ```

*   **Postman Setup:**
    *   Method: `GET`
    *   URL: `http://localhost:8000/api/v1/parse/statements/batch/<YOUR_BATCH_ID>` (Replace `<YOUR_BATCH_ID>`)
    *   Auth: `Bearer Token`
    *   Body: `none`

---

### 5. Invoice Parsing

Endpoints specific to submitting and managing invoice parsing jobs. These follow the same patterns as the Bank Statement endpoints.

#### 5.1 Submit Invoice Job

*   **Endpoint:** `POST /parse/invoices`
*   **Full URL:** `http://localhost:8000/api/v1/parse/invoices`
*   **Description:** Submits a previously uploaded invoice for asynchronous parsing.
*   **Authentication:** Required (`Bearer Token`)
*   **Example Request Body (JSON):**
    ```json
    {
      "file_id": "1713177600123456789_invoice.pdf", // file_id from upload response
      "document_id": "b1c2d3e4-f5g6-7890-1234-567890abcdef", // doc_id from upload response
      "schema_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef", // Optional custom invoice schema
      "callback_url": null
    }
    ```
*   **Example Success Response (202 Accepted):** (Similar to statements, returns `job_id`)
*   **Postman Setup:** (Similar to submitting a statement job)

#### 5.2 Get Invoice Job Status

*   **Endpoint:** `GET /parse/invoices/{job_id}`
*   **Full URL Example:** `http://localhost:8000/api/v1/parse/invoices/job_f1e2d3c4-b5a6-7890-1234-567890abcdef`
*   **Description:** Retrieves the status and results of a specific invoice job.
*   **Authentication:** Required (`Bearer Token`)
*   **Example Success Response (200 OK):** (Similar structure to getting statement job status, `result` contains parsed invoice data)
*   **Postman Setup:** (Similar to getting statement job status)

#### 5.3 List Invoice Jobs

*   **Endpoint:** `GET /parse/invoices`
*   **Full URL:** `http://localhost:8000/api/v1/parse/invoices`
*   **Description:** Lists invoice jobs for the user.
*   **Authentication:** Required (`Bearer Token`)
*   **Query Parameters:** `page`, `page_size`, `status`
*   **Example Success Response (200 OK):** (Similar structure to listing statement jobs)
*   **Postman Setup:** (Similar to listing statement jobs)

#### 5.4 Submit Invoice Batch Job

*   **Endpoint:** `POST /parse/invoices/batch`
*   **Full URL:** `http://localhost:8000/api/v1/parse/invoices/batch`
*   **Description:** Submits multiple invoices for batch processing.
*   **Authentication:** Required (`Bearer Token`)
*   **Example Request Body (JSON):** (Similar to statement batch, list invoice file/doc IDs)
*   **Example Success Response (202 Accepted):** (Returns `batch_id`)
*   **Postman Setup:** (Similar to submitting statement batch job)

#### 5.5 Get Invoice Batch Job Status

*   **Endpoint:** `GET /parse/invoices/batch/{batch_id}`
*   **Full URL Example:** `http://localhost:8000/api/v1/parse/invoices/batch/batch_g1h2i3j4-k5l6-7890-1234-567890abcdef`
*   **Description:** Retrieves the status of an invoice batch job.
*   **Authentication:** Required (`Bearer Token`)
*   **Example Success Response (200 OK):** (Similar structure to getting statement batch status)
*   **Postman Setup:** (Similar to getting statement batch status)

---

## Important Notes

*   **Dependencies:** This API relies heavily on external services:
    *   **Supabase:** For user authentication linkage, document metadata storage, and file storage.
    *   **Keyfolio:** For API key validation and rate limiting.
    *   **Redis:** For background job queuing (ARQ) and caching job/batch status.
    *   **Google Gemini API:** For the actual document parsing. Ensure you have valid API keys with billing enabled.
*   **Background Worker:** The `worker.py` process *must* be running for any submitted parsing jobs to be processed.
*   **Concurrency Model:** The worker uses a scatter-gather approach. `process_document` downloads the file and enqueues `process_page` tasks for each page. `process_page` calls Gemini. `assemble_job_results` gathers page results and updates the final job status.
*   **Error Handling:** Check API responses for success/error status and details. Logs provide more in-depth information (API logs in `api_logs` table in Supabase, worker logs in the console where `worker.py` is running). 