#!/usr/bin/env python3
"""
test our api/server
"""

import pytest
import httpx

# Test configuration
API_BASE_URL = "http://localhost:8000"

@pytest.fixture
def client():
    """HTTP client fixture"""
    return httpx.Client(base_url=API_BASE_URL)

class TestHealthAndInfo:
    """Test basic health and info endpoints"""
        
    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "investments_records" in data
        assert "summary_records" in data
    
    def test_data_summary(self, client):
        response = client.get("/data/summary")
        assert response.status_code == 200
        data = response.json()
        assert "investments_table" in data
        assert "summary_table" in data

class TestDataEndpoints:
    """Test main data query endpoints"""
    
    def test_investments_basic(self, client):
        response = client.get("/investments?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "pagination" in data
        assert data["data_source"] == "detailed_transactions"
        assert len(data["data"]) <= 5
    
    def test_investments_with_filters(self, client):
        response = client.get("/investments?state=Texas&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert data["filters_applied"]["state"] == "Texas"
        # Check that returned data actually matches filter
        for record in data["data"]:
            assert record.get("state_name") == "Texas"
    
    def test_summary_basic(self, client):
        response = client.get("/summary?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "pagination" in data
        assert data["data_source"] == "historical_summary"
        assert len(data["data"]) <= 5
    
    def test_summary_with_filters(self, client):
        response = client.get("/summary?state=California&fiscal_year=2023")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        filters = data["filters_applied"]
        assert filters.get("state") == "California"
        assert filters.get("fiscal_year") == 2023

class TestPostEndpoints:
    """Test POST endpoints with request bodies"""
    
    def test_investments_post_query(self, client):
        query = {
            "filters": {"state": "Texas"},
            "limit": 5,
            "offset": 0
        }
        response = client.post("/investments/query", json=query)
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert data["filters_applied"]["state"] == "Texas"
        assert len(data["data"]) <= 5
    
    def test_summary_post_query(self, client):
        query = {
            "filters": {"program": "Rural Housing"},
            "limit": 10
        }
        response = client.post("/summary/query", json=query)
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert data["data_source"] == "historical_summary"

class TestAggregationEndpoints:
    """Test aggregation and analysis endpoints"""
    
    def test_state_aggregations(self, client):
        response = client.get("/aggregations/states?state=Texas")
        assert response.status_code == 200
        data = response.json()
        assert data["aggregation_type"] == "state_summary"
        assert "data" in data
        assert "totals" in data
        assert data["query_params"]["state"] == "Texas"
    
    def test_program_aggregations(self, client):
        response = client.get("/aggregations/programs?program=Rural Housing")
        assert response.status_code == 200
        data = response.json()
        assert data["aggregation_type"] == "program_summary"
        assert "totals" in data
        assert data["query_params"]["program"] == "Rural Housing"
    
    def test_compare_states(self, client):
        response = client.get("/aggregations/compare?compare_type=states&items=Texas,California")
        assert response.status_code == 200
        data = response.json()
        assert data["comparison_type"] == "states"
        assert data["items_requested"] == ["Texas", "California"]
        assert "comparisons" in data
    
    def test_state_trends(self, client):
        response = client.get("/trends/states?state=Texas&years=3")
        assert response.status_code == 200
        data = response.json()
        assert data["state_name"] == "Texas"
        assert "trend_analysis" in data

class TestMetadataEndpoints:
    """Test metadata and utility endpoints"""
    
    def test_columns_endpoint(self, client):
        response = client.get("/data/columns")
        assert response.status_code == 200
        data = response.json()
        assert "investments_table" in data
        assert "summary_table" in data
        assert "columns" in data["investments_table"]
        assert "columns" in data["summary_table"]
    
    def test_stats_endpoint(self, client):
        response = client.get("/data/stats")
        assert response.status_code == 200
        data = response.json()
        assert "database_summary" in data
        assert "endpoints_available" in data
        assert "data_freshness" in data

class TestErrorHandling:
    """Test error conditions and edge cases"""
    
    def test_invalid_state(self, client):
        response = client.get("/investments?state=InvalidState")
        assert response.status_code == 200  # Should return empty results, not error
        data = response.json()
        assert len(data["data"]) == 0
    
    def test_invalid_compare_type(self, client):
        response = client.get("/aggregations/compare?compare_type=invalid&items=Texas")
        assert response.status_code == 400
    
    def test_large_limit(self, client):
        response = client.get("/investments?limit=2000")  # Over max
        assert response.status_code == 422  # FastAPI validation error
        data = response.json()
        assert "detail" in data  # Should contain validation error details

if __name__ == "__main__":
    print("Running API tests...")
    print("Make sure the API server is running on http://localhost:8000")
    print("Start with: uv run python api/server.py")
    pytest.main([__file__, "-v"])