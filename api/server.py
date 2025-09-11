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
    version="1.0.0" #idk how to keep this same as whole thing just leaving at 1
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
        "message": "Local USDA Rural Data Gateway API",
        "endpoints": {
            "/investments": "Get detailed investment transaction data",
            "/summary": "Get historical summary data",
            "/aggregations/states": "Get state-level aggregations",
            "/aggregations/programs": "Get program-level aggregations",
            "/aggregations/compare": "Compare states or programs",
            "/data/summary": "Get database summary statistics",
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
            "last_updated": summary["last_updated"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.get("/data/summary")
async def get_data_summary():
    """Get summary statistics of available data"""
    try:
        summary = processor.get_data_summary()
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting summary: {str(e)}")

@app.get("/investments")
async def get_investments(
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    state: Optional[str] = Query(None, description="Filter by state"),
    program: Optional[str] = Query(None, description="Filter by program area"),
    fiscal_year: Optional[int] = Query(None, description="Filter by fiscal year"),
    borrower_name: Optional[str] = Query(None, description="Search borrower name")
):
    """
    Get detailed investment transaction data with filtering and pagination
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
            
        result = processor.query_investments(filters, limit, offset)
        
        return {
            "data": result["data"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "returned": result["returned"]
            },
            "data_source": "detailed_transactions",
            "filters_applied": filters
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying investment data: {str(e)}")

@app.get("/summary")
async def get_summary_data(
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    state: Optional[str] = Query(None, description="Filter by state"),
    program: Optional[str] = Query(None, description="Filter by program area"),
    fiscal_year: Optional[int] = Query(None, description="Filter by fiscal year")
):
    """
    Get historical summary data (state + program + fiscal year aggregations)
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
            
        result = processor.query_summary(filters, limit, offset)
        
        return {
            "data": result["data"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "returned": result["returned"]
            },
            "data_source": "historical_summary",
            "filters_applied": filters
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying summary data: {str(e)}")

@app.post("/investments/query")
async def query_investments_advanced(query: DataQuery):
    """
    Advanced investment data querying with POST body
    """
    try:
        result = processor.query_investments(query.filters, query.limit or 100, query.offset or 0)
        
        return {
            "data": result["data"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "returned": result["returned"]
            },
            "data_source": "detailed_transactions",
            "filters_applied": query.filters
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying investment data: {str(e)}")

@app.post("/summary/query")
async def query_summary_advanced(query: DataQuery):
    """
    Advanced summary data querying with POST body
    """
    try:
        result = processor.query_summary(query.filters, query.limit or 100, query.offset or 0)
        
        return {
            "data": result["data"],
            "pagination": {
                "total": result["total"],
                "limit": result["limit"],
                "offset": result["offset"],
                "returned": result["returned"]
            },
            "data_source": "historical_summary",
            "filters_applied": query.filters
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying summary data: {str(e)}")

@app.get("/aggregations/states")
async def get_state_aggregations(
    state: Optional[str] = Query(None, description="Specific state name"),
    fiscal_year: Optional[int] = Query(None, description="Specific fiscal year")
):
    """
    Get state investment aggregations from SUMMARY table
    """
    try:
        filters = {}
        if state:
            filters['state'] = state
        if fiscal_year:
            filters['fiscal_year'] = fiscal_year
            
        result = processor.query_summary(filters, limit=1000, offset=0)
        
        # Calculate totals
        total_dollars = sum(row.get('investment_dollars_numeric', 0) or 0 for row in result['data'])
        total_investments = sum(row.get('number_of_investments', 0) or 0 for row in result['data'])
        
        return {
            "aggregation_type": "state_summary",
            "query_params": {"state": state, "fiscal_year": fiscal_year},
            "data": result['data'],
            "totals": {
                "total_investment_dollars": total_dollars,
                "total_number_of_investments": total_investments,
                "record_count": result['returned']
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting state aggregations: {str(e)}")

@app.get("/aggregations/programs")
async def get_program_aggregations(
    program: Optional[str] = Query(None, description="Specific program area"),
    fiscal_year: Optional[int] = Query(None, description="Specific fiscal year")
):
    """
    Get program investment aggregations from SUMMARY table
    """
    try:
        filters = {}
        if program:
            filters['program'] = program
        if fiscal_year:
            filters['fiscal_year'] = fiscal_year
            
        result = processor.query_summary(filters, limit=1000, offset=0)
        
        # Calculate totals
        total_dollars = sum(row.get('investment_dollars_numeric', 0) or 0 for row in result['data'])
        total_investments = sum(row.get('number_of_investments', 0) or 0 for row in result['data'])
        
        return {
            "aggregation_type": "program_summary",
            "query_params": {"program": program, "fiscal_year": fiscal_year},
            "data": result['data'],
            "totals": {
                "total_investment_dollars": total_dollars,
                "total_number_of_investments": total_investments,
                "record_count": result['returned']
            }
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
    Compare multiple states or programs using pre-computed aggregations
    """
    # Validate compare_type first, outside try/catch
    if compare_type not in ['states', 'programs']:
        raise HTTPException(status_code=400, detail="compare_type must be 'states' or 'programs'")
    
    try:
        items_list = [item.strip() for item in items.split(',')]
        comparisons = []
        
        if compare_type == 'states':
            for state in items_list:
                filters = {'state': state}
                if fiscal_year:
                    filters['fiscal_year'] = fiscal_year
                result = processor.query_summary(filters, limit=1000, offset=0)
                if result['data']:
                    total_dollars = sum(row.get('investment_dollars_numeric', 0) or 0 for row in result['data'])
                    total_investments = sum(row.get('number_of_investments', 0) or 0 for row in result['data'])
                    comparisons.append({
                        "state": state,
                        "data": result['data'],
                        "totals": {
                            "total_investment_dollars": total_dollars,
                            "total_number_of_investments": total_investments
                        }
                    })
        elif compare_type == 'programs':
            for program in items_list:
                filters = {'program': program}
                if fiscal_year:
                    filters['fiscal_year'] = fiscal_year
                result = processor.query_summary(filters, limit=1000, offset=0)
                if result['data']:
                    total_dollars = sum(row.get('investment_dollars_numeric', 0) or 0 for row in result['data'])
                    total_investments = sum(row.get('number_of_investments', 0) or 0 for row in result['data'])
                    comparisons.append({
                        "program": program,
                        "data": result['data'],
                        "totals": {
                            "total_investment_dollars": total_dollars,
                            "total_number_of_investments": total_investments
                        }
                    })
            
        return {
            "comparison_type": compare_type,
            "items_requested": items_list,
            "fiscal_year": fiscal_year,
            "comparisons": comparisons,
            "count": len(comparisons)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error comparing aggregations: {str(e)}")

@app.get("/data/columns")
async def get_available_columns():
    """
    Get list of available data columns for both investments and summary tables
    """
    try:
        # Define the available columns based on our schema
        investments_columns = [
            "fiscal_year", "state_name", "county", "county_fips", "congressional_district",
            "program_area", "program", "zip_code", "persistent_poverty_community_status",
            "borrower_name", "project_name", "investment_type", "city", "lender_name",
            "funding_code", "naics_industry_sector", "naics_national_industry_code",
            "naics_national_industry", "portfolio_type", "project_announced_description",
            "investment_dollars_numeric", "number_of_investments"
        ]
        
        summary_columns = [
            "fiscal_year", "state_name", "program_area", "investment_dollars_numeric",
            "number_of_investments", "persistent_poverty_investment_dollars_numeric"
        ]
        
        return {
            "investments_table": {
                "columns": investments_columns,
                "total_columns": len(investments_columns),
                "description": "Detailed transaction-level data"
            },
            "summary_table": {
                "columns": summary_columns,
                "total_columns": len(summary_columns),
                "description": "Historical state+program aggregations"
            }
        }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting columns: {str(e)}")

@app.get("/data/stats")
async def get_data_statistics():
    """
    Get comprehensive statistics about the data
    """
    try:
        summary = processor.get_data_summary()
        
        # Get some additional stats for context
        return {
            "database_summary": summary,
            "endpoints_available": {
                "detailed_data": "/investments",
                "historical_summaries": "/summary", 
                "state_aggregations": "/aggregations/states",
                "program_aggregations": "/aggregations/programs",
                "comparisons": "/aggregations/compare"
            },
            "data_freshness": {
                "investments_years": summary["investments_table"]["fiscal_year_range"],
                "summary_years": summary["summary_table"]["fiscal_year_range"]
            }
        }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting statistics: {str(e)}")

# Additional endpoint for trends analysis
@app.get("/trends/states")
async def get_state_trends(
    state: str = Query(..., description="State name"),
    years: int = Query(5, ge=2, le=20, description="Number of recent years to analyze")
):
    """
    Get investment trends for a specific state over recent years
    """
    try:
        result = processor.query_summary({'state': state}, limit=1000, offset=0)
        
        if result['data']:
            # Group by fiscal year and calculate yearly totals
            yearly_data = {}
            for row in result['data']:
                fy = row.get('fiscal_year')
                if fy:
                    if fy not in yearly_data:
                        yearly_data[fy] = {'fiscal_year': fy, 'total_dollars': 0, 'total_investments': 0, 'programs': []}
                    yearly_data[fy]['total_dollars'] += row.get('investment_dollars_numeric', 0) or 0
                    yearly_data[fy]['total_investments'] += row.get('number_of_investments', 0) or 0
                    yearly_data[fy]['programs'].append(row.get('program_area', ''))
            
            # Sort by fiscal year and take the most recent years
            recent_years = sorted(yearly_data.values(), key=lambda x: x['fiscal_year'], reverse=True)[:years]
            
            return {
                "state_name": state,
                "trend_analysis": {
                    "years_analyzed": len(recent_years),
                    "yearly_data": recent_years
                }
            }
        else:
            return {"error": f"No trend data available for {state}"}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting state trends: {str(e)}")

if __name__ == "__main__":
    print("Starting USDA Rural Data Gateway API...")
    print("API will be available at http://localhost:8000")
    print("Interactive docs will be available at http://localhost:8000/docs") #swagger
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )