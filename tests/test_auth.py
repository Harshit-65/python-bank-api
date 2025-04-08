import pytest
import respx
from httpx import Response
from fastapi import FastAPI, Depends, status
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from app.auth.dependencies import verify_api_key, AuthData, APIKeyNotFound, KeyfolioVerificationError, InvalidTokenError, RateLimitExceededError
from app.auth.models import KeyfolioResponse
from app.db.supabase_client import get_supabase_client
from app.config import KEYFOLIO_URL

# Create a dummy FastAPI app for testing the dependency
app = FastAPI()

@app.get("/secure")
def secure_endpoint(current_user: AuthData):
    return {"user": current_user}

client = TestClient(app)

# Fixture to mock Supabase client
@pytest.fixture
def mock_supabase():
    mock = AsyncMock()
    # Configure the mock's execute method to return an AsyncMock
    execute_mock = AsyncMock()
    mock.table.return_value.select.return_value.eq.return_value.execute = execute_mock
    return mock, execute_mock

@pytest.fixture(autouse=True)
def override_supabase_dependency(mock_supabase):
    mock_client, _ = mock_supabase
    app.dependency_overrides[get_supabase_client] = lambda: mock_client
    yield
    app.dependency_overrides = {}

# --- Test Cases ---

@respx.mock
def test_auth_success(mock_supabase):
    mock_client, mock_execute = mock_supabase
    key = "valid_key"
    user_id = "user_123"
    api_key_id = "key_abc"

    # Mock Keyfolio response
    keyfolio_route = respx.post(f"{KEYFOLIO_URL}/api/verify").mock(
        return_value=Response(200, json=KeyfolioResponse(valid=True, limit=100, remaining=99, reset_after_ms=60000).model_dump())
    )
    # Mock Supabase response
    supabase_response_mock = AsyncMock()
    supabase_response_mock.data = [{"id": api_key_id, "user_id": user_id}]
    mock_execute.return_value = supabase_response_mock

    response = client.get("/secure", headers={"Authorization": f"Bearer {key}"})

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"user": {"user_id": user_id, "api_key_id": api_key_id}}
    assert keyfolio_route.called
    mock_client.table.assert_called_once_with("api_keys")
    mock_client.table().select().eq.assert_called_once_with("key_hash", key)
    mock_execute.assert_awaited_once()


def test_auth_no_header():
    response = client.get("/secure")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json()["detail"]["code"] == "MISSING_AUTH_HEADER"

@respx.mock
def test_auth_keyfolio_network_error(mock_supabase):
    key = "any_key"
    respx.post(f"{KEYFOLIO_URL}/api/verify").mock(side_effect=Exception("Network Error"))

    response = client.get("/secure", headers={"Authorization": f"Bearer {key}"})

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json()["detail"]["code"] == "INTERNAL_SERVER_ERROR"

@respx.mock
def test_auth_keyfolio_invalid_response(mock_supabase):
    key = "any_key"
    respx.post(f"{KEYFOLIO_URL}/api/verify").mock(return_value=Response(200, text="not json"))

    response = client.get("/secure", headers={"Authorization": f"Bearer {key}"})

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json()["detail"]["code"] == "INTERNAL_SERVER_ERROR"

@respx.mock
def test_auth_keyfolio_invalid_key(mock_supabase):
    key = "invalid_key"
    respx.post(f"{KEYFOLIO_URL}/api/verify").mock(
        return_value=Response(200, json=KeyfolioResponse(valid=False, limit=0, remaining=0, reset_after_ms=0).model_dump())
    )

    response = client.get("/secure", headers={"Authorization": f"Bearer {key}"})

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json()["detail"]["code"] == "INVALID_TOKEN"

@respx.mock
def test_auth_keyfolio_rate_limit(mock_supabase):
    key = "rate_limited_key"
    reset_ms = 30000
    respx.post(f"{KEYFOLIO_URL}/api/verify").mock(
        return_value=Response(200, json=KeyfolioResponse(valid=True, limit=100, remaining=0, reset_after_ms=reset_ms).model_dump())
    )

    response = client.get("/secure", headers={"Authorization": f"Bearer {key}"})

    assert response.status_code == status.HTTP_429_TOO_MANY_REQUESTS
    assert response.json()["detail"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert response.headers["retry-after"] == str(reset_ms // 1000)

@respx.mock
def test_auth_supabase_key_not_found(mock_supabase):
    mock_client, mock_execute = mock_supabase
    key = "key_not_in_db"

    # Mock Keyfolio success
    respx.post(f"{KEYFOLIO_URL}/api/verify").mock(
        return_value=Response(200, json=KeyfolioResponse(valid=True, limit=100, remaining=99, reset_after_ms=60000).model_dump())
    )
    # Mock Supabase empty response
    supabase_response_mock = AsyncMock()
    supabase_response_mock.data = []
    mock_execute.return_value = supabase_response_mock

    response = client.get("/secure", headers={"Authorization": f"Bearer {key}"})

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json()["detail"]["code"] == "INVALID_TOKEN"
    assert "API key not found in database" in response.json()["detail"]["details"]

@respx.mock
def test_auth_supabase_error(mock_supabase):
    mock_client, mock_execute = mock_supabase
    key = "db_error_key"

    # Mock Keyfolio success
    respx.post(f"{KEYFOLIO_URL}/api/verify").mock(
        return_value=Response(200, json=KeyfolioResponse(valid=True, limit=100, remaining=99, reset_after_ms=60000).model_dump())
    )
    # Mock Supabase exception
    mock_execute.side_effect = Exception("DB Connection Error")

    response = client.get("/secure", headers={"Authorization": f"Bearer {key}"})

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert response.json()["detail"]["code"] == "INTERNAL_SERVER_ERROR"
    assert "Error fetching user from database" in response.json()["detail"]["details"] 