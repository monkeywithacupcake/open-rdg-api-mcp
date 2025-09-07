#!/usr/bin/env python3
"""
FastAPI server for serving USDA Rural Development data
Provides REST API endpoints for querying processed data
This is an API server that has to run on local machine
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Dict, List, Any
import uvicorn
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from api.data_processor import USDADataProcessor

app = FastAPI(
    title="USDA Local Rural Data Gateway API",
    description="API for accessing USDA Rural Investment data",
    version="1.0.0"
)

# Initialize data processor
processor = USDADataProcessor()

class DataQuery(BaseModel):
    filters: Optional[Dict[str, Any]] = None
    limit: Optional[int] = 100
    offset: Optional[int] = 0

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "USDA Local Rural Data Gateway API",
        "version": "1.0.0",
        "endpoints": {
            "/data": "Get rural investment data",
            "/summary": "Get data summary statistics",
            "/health": "Health check"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        summary = processor.get_data_summary()
        return {
            "status": "healthy",
            "database": "connected",
            "total_records": summary.get("total_imports", 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.get("/summary")
async def get_data_summary():
    """Get summary statistics of available data"""
    try:
        summary = processor.get_data_summary()
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting summary: {str(e)}")

@app.get("/data")
async def get_data(
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    state: Optional[str] = Query(None, description="Filter by state"),
    program: Optional[str] = Query(None, description="Filter by program type"),
    fiscal_year: Optional[int] = Query(None, description="Filter by fiscal year"),
    borrower_name: Optional[str] = Query(None, description="Search borrower name")
):
    """
    Get rural investment data with fast indexed filtering
    """
    try:
        # Build filters dict from query parameters
        filters = {}
        if state:
            filters['state'] = state
        if program:
            filters['program'] = program
        if fiscal_year:
            filters['fiscal_year'] = fiscal_year
        if borrower_name:
            filters['borrower_name'] = borrower_name
            
        result = processor.query_structured_data(filters, limit, offset)
        
        return {
            "data": result["data"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "returned": result["returned"]
            },
            "query_type": "structured",
            "filters_applied": filters
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying data: {str(e)}")

@app.post("/data/query")
async def query_data(query: DataQuery):
    """
    Advanced data querying with POST body
    """
    try:
        data = processor.query_data(query.filters)
        
        # Apply pagination
        total = len(data)
        start = query.offset or 0
        end = start + (query.limit or 100)
        paginated_data = data[start:end]
        
        return {
            "data": paginated_data,
            "pagination": {
                "total": total,
                "limit": query.limit,
                "offset": query.offset,
                "returned": len(paginated_data)
            },
            "filters_applied": query.filters
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying data: {str(e)}")

@app.get("/data/columns")
async def get_available_columns():
    """
    Get list of available data columns for filtering
    """
    try:
        summary = processor.get_data_summary()
        latest_import = summary.get("latest_import")
        
        if latest_import and "columns" in latest_import:
            return {
                "columns": latest_import["columns"],
                "total_columns": len(latest_import["columns"])
            }
        else:
            return {"message": "No data available yet"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting columns: {str(e)}")

@app.get("/aggregations/states")
async def get_state_aggregations(
    state: Optional[str] = Query(None, description="Specific state name"),
    fiscal_year: Optional[int] = Query(None, description="Specific fiscal year")
):
    """
    Get pre-computed state investment aggregations (fast)
    """
    try:
        result = processor.get_state_summary(state, fiscal_year)
        return {
            "aggregation_type": "state_summary",
            "query_params": {"state": state, "fiscal_year": fiscal_year},
            **result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting state aggregations: {str(e)}")

@app.get("/aggregations/programs")
async def get_program_aggregations(
    program: Optional[str] = Query(None, description="Specific program area"),
    fiscal_year: Optional[int] = Query(None, description="Specific fiscal year")
):
    """
    Get pre-computed program investment aggregations (fast)
    """
    try:
        result = processor.get_program_summary(program, fiscal_year)
        return {
            "aggregation_type": "program_summary",
            "query_params": {"program": program, "fiscal_year": fiscal_year},
            **result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting program aggregations: {str(e)}")

@app.get("/aggregations/compare")
async def compare_aggregations(
    compare_type: str = Query(..., description="Type: 'states' or 'programs'"),
    items: str = Query(..., description="Comma-separated list to compare (e.g., 'Texas,California')"),
    fiscal_year: Optional[int] = Query(None, description="Specific fiscal year")
):
    """
    Compare multiple states or programs (fast aggregation-based)
    """
    try:
        items_list = [item.strip() for item in items.split(',')]
        comparisons = []
        
        if compare_type == 'states':
            for state in items_list:
                result = processor.get_state_summary(state, fiscal_year)
                if not result.get('error'):
                    comparisons.append(result)
        elif compare_type == 'programs':
            for program in items_list:
                result = processor.get_program_summary(program, fiscal_year)
                if not result.get('error'):
                    comparisons.append(result)
        else:
            raise HTTPException(status_code=400, detail="compare_type must be 'states' or 'programs'")
            
        return {
            "comparison_type": compare_type,
            "items_requested": items_list,
            "fiscal_year": fiscal_year,
            "comparisons": comparisons,
            "count": len(comparisons)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error comparing aggregations: {str(e)}")

if __name__ == "__main__":
    print("Starting USDA Rural Data Gateway API...")
    print("API will be available at http://localhost:8000")
    
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )