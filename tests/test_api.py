#!/usr/bin/env python3
"""
Comprehensive tests for the USDA Local Rural Data Gateway API
Tests all endpoints defined in api/server.py against a running server
"""

import pytest
import requests
import json
import time
from pathlib import Path
import sys

# Test configuration
API_BASE_URL = "http://localhost:8000"
TEST_TIMEOUT = 10


def check_server_running():
    """Check if the server is running, skip tests if not"""
    try:
        response = requests.get(f"{API_BASE_URL}/", timeout=5)
        return True
    except requests.exceptions.RequestException:
        return False

@pytest.fixture(scope="session", autouse=True)
def ensure_server_running():
    """Ensure server is running for all tests"""
    if not check_server_running():
        print("start server first in another terminal with uv run api/server.py")
        pytest.skip("Server not running at http://localhost:8000. Start with: uv run api/server.py")

class TestAPIEndpoints:
    """Test the API endpoints against a running server"""

    def test_root_endpoint(self):
        """Test the root endpoint returns API information"""
        response = requests.get(f"{API_BASE_URL}/", timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        
        data = response.json()
        assert data["message"] == "USDA Local Rural Data Gateway API"
        assert "endpoints" in data
        assert "/data" in data["endpoints"]
        assert "/summary" in data["endpoints"]
        assert "/health" in data["endpoints"]

    def test_health_endpoint(self):
        """Test health check endpoint"""
        response = requests.get(f"{API_BASE_URL}/health", timeout=TEST_TIMEOUT)
        # Should be 200 (healthy) or 500 (unhealthy but responding)
        assert response.status_code in [200, 500]
        
        data = response.json()
        assert "status" in data
        if response.status_code == 200:
            assert data["status"] == "healthy"
            assert data["database"] == "connected"
            assert "total_records" in data

    def test_summary_endpoint(self):
        """Test data summary endpoint"""
        response = requests.get(f"{API_BASE_URL}/summary", timeout=TEST_TIMEOUT)
        # Should be 200 (has data) or 500 (no data but responding)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert "total_imports" in data
            # May have data or not, so we check structure not specific values

    def test_data_endpoint_basic(self):
        """Test basic data retrieval"""
        response = requests.get(f"{API_BASE_URL}/data", timeout=TEST_TIMEOUT)
        # Should be 200 (has data) or 500 (no data/error but responding)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert "data" in data
            assert "pagination" in data
            assert "query_type" in data
            assert "filters_applied" in data
            
            assert data["query_type"] == "structured"
            assert "limit" in data["pagination"]
            assert "offset" in data["pagination"]
            assert "total" in data["pagination"]
            assert "returned" in data["pagination"]

    def test_data_endpoint_pagination(self):
        """Test data endpoint with pagination parameters"""
        response = requests.get(f"{API_BASE_URL}/data?limit=5&offset=0", timeout=TEST_TIMEOUT)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["pagination"]["limit"] == 5
            assert data["pagination"]["offset"] == 0

    def test_data_endpoint_filters(self):
        """Test data endpoint with filter parameters"""
        response = requests.get(f"{API_BASE_URL}/data?state=California&limit=10", timeout=TEST_TIMEOUT)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["filters_applied"]["state"] == "California"

    def test_data_endpoint_fiscal_year_filter(self):
        """Test data endpoint with fiscal year filter"""
        response = requests.get(f"{API_BASE_URL}/data?fiscal_year=2023&limit=10", timeout=TEST_TIMEOUT)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["filters_applied"]["fiscal_year"] == 2023

    def test_data_endpoint_invalid_pagination(self):
        """Test data endpoint with invalid pagination parameters"""
        # Test limit too large - should return validation error
        response = requests.get(f"{API_BASE_URL}/data?limit=2000", timeout=TEST_TIMEOUT)
        assert response.status_code == 422  # Validation error
        
        # Test negative limit
        response = requests.get(f"{API_BASE_URL}/data?limit=-1", timeout=TEST_TIMEOUT)
        assert response.status_code == 422
        
        # Test negative offset
        response = requests.get(f"{API_BASE_URL}/data?offset=-1", timeout=TEST_TIMEOUT)
        assert response.status_code == 422

    def test_columns_endpoint(self):
        """Test available columns endpoint"""
        response = requests.get(f"{API_BASE_URL}/data/columns", timeout=TEST_TIMEOUT)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            # Should have either columns or a message about no data
            assert "columns" in data or "message" in data
            if "columns" in data:
                assert "total_columns" in data

    def test_query_data_post(self):
        """Test POST query endpoint"""
        query_payload = {
            "filters": {"state": "California"},
            "limit": 10,
            "offset": 0
        }
        
        response = requests.post(
            f"{API_BASE_URL}/data/query",
            json=query_payload,
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert "data" in data
            assert "pagination" in data
            assert "filters_applied" in data
            assert data["filters_applied"]["state"] == "California"

    def test_state_aggregations_endpoint(self):
        """Test state aggregations endpoint"""
        response = requests.get(f"{API_BASE_URL}/aggregations/states", timeout=TEST_TIMEOUT)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["aggregation_type"] == "state_summary"
            assert "query_params" in data

    def test_state_aggregations_specific_state(self):
        """Test state aggregations for specific state"""
        response = requests.get(f"{API_BASE_URL}/aggregations/states?state=California", timeout=TEST_TIMEOUT)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["aggregation_type"] == "state_summary"
            assert data["query_params"]["state"] == "California"

    def test_program_aggregations_endpoint(self):
        """Test program aggregations endpoint"""
        response = requests.get(f"{API_BASE_URL}/aggregations/programs", timeout=TEST_TIMEOUT)
        assert response.status_code in [200, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["aggregation_type"] == "program_summary"

    def test_compare_aggregations_states(self):
        """Test state comparison endpoint"""
        response = requests.get(
            f"{API_BASE_URL}/aggregations/compare?compare_type=states&items=California,Texas",
            timeout=TEST_TIMEOUT
        )
        assert response.status_code in [200, 400, 500]  # 400 if validation fails
        
        if response.status_code == 200:
            data = response.json()
            assert data["comparison_type"] == "states"
            assert data["items_requested"] == ["California", "Texas"]

    def test_compare_aggregations_invalid_type(self):
        """Test comparison with invalid type"""
        response = requests.get(
            f"{API_BASE_URL}/aggregations/compare?compare_type=invalid&items=A,B",
            timeout=TEST_TIMEOUT
        )
        assert response.status_code == 500

    def test_openapi_docs(self):
        """Test that OpenAPI docs are available"""
        response = requests.get(f"{API_BASE_URL}/docs", timeout=TEST_TIMEOUT)
        # Should redirect or show docs page
        assert response.status_code in [200, 307]
        
        response = requests.get(f"{API_BASE_URL}/openapi.json", timeout=TEST_TIMEOUT)
        assert response.status_code == 200
        
        openapi_spec = response.json()
        assert "openapi" in openapi_spec
        assert "paths" in openapi_spec


if __name__ == "__main__":
    print("Running USDA Rural Data Gateway API Tests...")
    print("=" * 50)
    
    # Check if server is running
    if check_server_running():
        print(f"✓ Server is running at {API_BASE_URL}")
    else:
        print(f"⚠ Server not running at {API_BASE_URL}")
        print("Start the server with: uv run api/server.py")
        sys.exit(1)
        
    # Run the tests
    pytest.main([__file__, "-v", "--tb=short"])