#!/usr/bin/env python3
"""
USDA Rural Data Gateway MCP Server
Provides LLMs with access to USDA Rural Investment data through semantic tools
"""

from typing import Optional, Dict, List, Any
import httpx
from pathlib import Path
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from fastmcp import FastMCP

# Initialize MCP server
mcp = FastMCP("USDA Rural Data Gateway")

# Configuration
API_BASE_URL = "http://localhost:8000"
DEFAULT_LIMIT = 10
MAX_LIMIT = 100

# Smart defaults configuration
SMART_DEFAULTS = {
    "summary_response": {
        "prefer_aggregation": True,
        "include_top_programs": 3,
        "include_recent_years": 3
    },
    "details_response": {
        "default_sort": "investment_dollars_numeric",
        "sort_order": "desc",
        "include_context": True
    },
    "comparison_limits": {
        "max_items": 10,
        "min_items": 2
    }
}


class USDAMCPClient:
    """Client for communicating with our local USDA API"""
    
    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url
        self.client = httpx.AsyncClient()
    
    async def health_check(self) -> bool:
        """Check if API is available"""
        try:
            response = await self.client.get(f"{self.base_url}/health")
            return response.status_code == 200
        except:
            return False
    
    async def get_data_summary(self) -> dict:
        """Get overall data summary"""
        response = await self.client.get(f"{self.base_url}/data/summary")
        response.raise_for_status()
        return response.json()
    
    async def query_investments_data(
        self, 
        filters: Dict[str, Any], 
        limit: int = DEFAULT_LIMIT, 
        offset: int = 0
    ) -> dict:
        """Query detailed investment transaction data"""
        params = {
            "limit": min(limit, MAX_LIMIT),
            "offset": offset,
            **filters
        }
        
        response = await self.client.get(f"{self.base_url}/investments", params=params)
        response.raise_for_status()
        return response.json()
    
    async def query_summary_data(
        self, 
        filters: Dict[str, Any], 
        limit: int = DEFAULT_LIMIT, 
        offset: int = 0
    ) -> dict:
        """Query historical summary data"""
        params = {
            "limit": min(limit, MAX_LIMIT),
            "offset": offset,
            **filters
        }
        
        response = await self.client.get(f"{self.base_url}/summary", params=params)
        response.raise_for_status()
        return response.json()
    
    async def get_state_aggregations(
        self, 
        state: Optional[str] = None, 
        fiscal_year: Optional[int] = None
    ) -> dict:
        """Get pre-computed state aggregations"""
        params = {}
        if state:
            params["state"] = state
        if fiscal_year:
            params["fiscal_year"] = fiscal_year
            
        response = await self.client.get(f"{self.base_url}/aggregations/states", params=params)
        response.raise_for_status()
        return response.json()
    
    async def get_program_aggregations(
        self, 
        program: Optional[str] = None, 
        fiscal_year: Optional[int] = None
    ) -> dict:
        """Get pre-computed program aggregations"""
        params = {}
        if program:
            params["program"] = program
        if fiscal_year:
            params["fiscal_year"] = fiscal_year
            
        response = await self.client.get(f"{self.base_url}/aggregations/programs", params=params)
        response.raise_for_status()
        return response.json()
    
    async def compare_aggregations(
        self,
        compare_type: str,
        items: List[str],
        fiscal_year: Optional[int] = None
    ) -> dict:
        """Compare multiple states or programs"""
        params = {
            "compare_type": compare_type,
            "items": ",".join(items)
        }
        if fiscal_year:
            params["fiscal_year"] = fiscal_year
            
        response = await self.client.get(f"{self.base_url}/aggregations/compare", params=params)
        response.raise_for_status()
        return response.json()

# Initialize API client
api_client = USDAMCPClient()

@mcp.tool()
async def get_rural_data(
    location: Optional[str] = None,
    program: Optional[str] = None,
    fiscal_year: Optional[int] = None,
    response_type: str = "summary",
    data_source: str = "auto",
    limit: int = DEFAULT_LIMIT
) -> dict:
    """
    Main tool for rural investment data queries with dual dataset support
    
    Args:
        location: State name or abbreviation (e.g., 'California', 'CA', 'Texas')
        program: Program area or type (e.g., 'housing', 'broadband', 'electric')
        fiscal_year: Specific fiscal year (e.g., 2023, 2024)
        response_type: Type of response - 'summary' for aggregated data, 'details' for individual records
        data_source: 'detailed' for transaction data, 'historical' for summary data, 'auto' for best choice
        limit: Maximum number of individual records to return (max 100)
    
    Returns:
        Dictionary containing investment data, aggregations, and metadata
    """
    
    try:
        # Check API health first
        if not await api_client.health_check():
            return {
                "error": "Enhanced USDA data API is not available. Please ensure the API server is running on port 8001.",
                "status": "api_unavailable"
            }
        
        # Resolve location to standard state name
        resolved_location = await _resolve_location_name(location) if location else None
        
        # Resolve program to standard program area name
        resolved_program = await _resolve_program_name(program) if program else None
        
        # Build filters
        filters = {}
        if resolved_location:
            filters["state"] = resolved_location
        if resolved_program:
            filters["program"] = resolved_program
        if fiscal_year:
            filters["fiscal_year"] = fiscal_year
        
        # Choose data source intelligently if set to 'auto'
        if data_source == "auto":
            # Use historical for summary requests, detailed for specific queries
            if response_type == "summary" and not fiscal_year:
                chosen_source = "historical"
            else:
                chosen_source = "detailed"
        else:
            chosen_source = data_source
        
        if response_type == "summary" and resolved_location:
            # Use fast aggregation endpoint for summary requests
            result = await api_client.get_state_aggregations(
                state=resolved_location,
                fiscal_year=fiscal_year
            )
            
            # Get data freshness info
            freshness_info = await _get_data_freshness_info()
            
            # Extract aggregation data from new API format
            aggregation_data = {}
            total_investments = 0
            total_dollars = 0.0
            
            if "totals" in result:
                # New API format with totals structure
                totals = result["totals"]
                total_investments = totals.get("total_number_of_investments", 0)
                total_dollars = totals.get("total_investment_dollars", 0.0)
                avg_investment = total_dollars / total_investments if total_investments > 0 else 0.0
            else:
                # Fallback - calculate from data array
                data_array = result.get("data", [])
                for record in data_array:
                    total_investments += record.get("number_of_investments", 0) or 0
                    total_dollars += record.get("investment_dollars_numeric", 0.0) or 0.0
                avg_investment = total_dollars / total_investments if total_investments > 0 else 0.0
            
            aggregation_data = {
                "total_investment_dollars": total_dollars,
                "total_investments": total_investments,
                "average_investment": avg_investment
            }
            
            # Format aggregation result with proper response structure
            formatted_result = {
                "query_context": {
                    "original_location": location,
                    "resolved_location": resolved_location,
                    "resolved_program": resolved_program,
                    "fiscal_year": fiscal_year,
                    "response_type": response_type,
                    "data_source_used": "aggregated"
                },
                "records": [{
                    "state_name": resolved_location,
                    "total_investments": total_investments,
                    "total_dollars": total_dollars,
                    "average_investment": avg_investment,
                    "fiscal_year_range": str(fiscal_year) if fiscal_year else "Multiple years"
                }],
                "response_context": {
                    "summary": {
                        "total_matching_records": total_investments,
                        "records_returned": 1,
                        "data_freshness": freshness_info.get("freshness_status", "unknown"),
                        "response_time_ms": 0  # Will be updated by caller
                    },
                    "aggregations": aggregation_data
                },
                "suggestions": {
                    "optimization": "Use response_type='details' to see individual investments",
                    "related_queries": [
                        f"Compare {resolved_location} with other large states",
                        f"See {resolved_location} program breakdown",
                        f"Find largest recipients in {resolved_location}"
                    ]
                }
            }
            
            return formatted_result
        
        else:
            # Apply smart defaults before querying
            total_matching_estimate = 1000  # We'll get the actual count from the query
            smart_defaults = _apply_smart_defaults(response_type, limit, filters, total_matching_estimate)
            adjusted_limit = smart_defaults["limit"]
            
            # Choose the appropriate endpoint based on data source
            if chosen_source == "historical":
                result = await api_client.query_summary_data(filters, adjusted_limit, 0)
                source_used = "historical_summary"
            else:
                result = await api_client.query_investments_data(filters, adjusted_limit, 0)
                source_used = "detailed_transactions"
            
            # Now we have the actual total count
            actual_total = result["pagination"]["total"]
            
            # Re-apply smart defaults with actual count if needed
            if actual_total != total_matching_estimate:
                smart_defaults = _apply_smart_defaults(response_type, limit, filters, actual_total)
                        
            # Format response with rich metadata and context
            query_metadata = {
                "location_requested": location,
                "location_resolved": resolved_location,
                "program_requested": program,
                "program_resolved": resolved_program,
                "fiscal_year": fiscal_year,
                "response_type": response_type,
                "data_source_requested": data_source,
                "data_source_used": source_used,
                "filters_applied": filters,
                "limit_requested": limit,
                "limit_applied": adjusted_limit
            }
            
            # Get data freshness info
            freshness_info = await _get_data_freshness_info()
            
            additional_context = {
                "summary": {
                    "total_matching_records": result["pagination"]["total"],
                    "records_returned": result["pagination"]["returned"],
                    "showing": f"{result['pagination']['returned']} of {result['pagination']['total']} matching investments"
                },
                "data_freshness": freshness_info
            }
            
            formatted_result = _format_response_with_context(
                result["data"], 
                query_metadata, 
                smart_defaults, 
                additional_context
            )
            
            return formatted_result
    
    except Exception as e:
        return {
            "error": f"Error querying rural investment data: {str(e)}",
            "status": "query_error"
        }

@mcp.tool()
async def get_summary_data(
    location: Optional[str] = None,
    program: Optional[str] = None,
    fiscal_year: Optional[int] = None,
    limit: int = DEFAULT_LIMIT
) -> dict:
    """
    Get historical summary data (state + program + fiscal year aggregations)
    
    This tool specifically accesses the historical summary dataset which contains
    10+ years of aggregated data by state and program area.
    
    Args:
        location: State name or abbreviation to filter by
        program: Program area to filter by
        fiscal_year: Specific fiscal year to filter by
        limit: Maximum number of summary records to return
    
    Returns:
        Dictionary containing historical summary data and metadata
    """
    
    try:
        if not await api_client.health_check():
            return {
                "error": "Local USDA data API is not available. Please ensure the API server is running on port 8001.",
                "status": "api_unavailable"
            }
        
        # Resolve filters
        resolved_location = await _resolve_location_name(location) if location else None
        resolved_program = await _resolve_program_name(program) if program else None
        
        # Build filters
        filters = {}
        if resolved_location:
            filters["state"] = resolved_location
        if resolved_program:
            filters["program"] = resolved_program
        if fiscal_year:
            filters["fiscal_year"] = fiscal_year
        
        # Query summary data
        result = await api_client.query_summary_data(filters, limit, 0)
        
        # Format response
        return {
            "query_metadata": {
                "location_requested": location,
                "location_resolved": resolved_location,
                "program_requested": program,
                "program_resolved": resolved_program,
                "fiscal_year": fiscal_year,
                "data_source": "historical_summary",
                "filters_applied": filters
            },
            "data": result["data"],
            "pagination": result["pagination"],
            "data_characteristics": {
                "data_type": "historical_aggregated",
                "granularity": "state + program + fiscal_year",
                "time_span": "10+ fiscal years",
                "includes_persistent_poverty_data": True
            }
        }
        
    except Exception as e:
        return {
            "error": f"Error querying summary data: {str(e)}",
            "status": "summary_query_error"
        }

# Keep all the existing helper functions with minimal changes
async def _resolve_location_name(location: str) -> Optional[str]:
    """
    Resolve location name/abbreviation to standard state name
    
    Handles all US state abbreviations and common name variations
    """
    if not location:
        return None
        
    # Complete US state abbreviation mappings
    state_mappings = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
        "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
        "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
        "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
        "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
        "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam", "AS": "American Samoa",
        "MP": "Northern Mariana Islands"
    }
    
    # Common name variations
    name_variations = {
        "washington state": "Washington",
        "wash": "Washington",
        "virginia": "Virginia",
        "west va": "West Virginia",
        "wva": "West Virginia",
        "n carolina": "North Carolina",
        "s carolina": "South Carolina",
        "n dakota": "North Dakota", 
        "s dakota": "South Dakota",
        "new mex": "New Mexico",
        "mass": "Massachusetts"
    }
    
    # Clean and normalize input
    location_clean = location.strip()
    location_upper = location_clean.upper()
    location_lower = location_clean.lower()
    
    # Check abbreviation mapping first
    if location_upper in state_mappings:
        return state_mappings[location_upper]
    
    # Check name variations
    if location_lower in name_variations:
        return name_variations[location_lower]
    
    # Check if input already matches a standard state name (case-insensitive)
    standard_states = set(state_mappings.values())
    for state in standard_states:
        if location_clean.lower() == state.lower():
            return state
    
    # Otherwise, title-case the input and return it
    # This handles cases where user provides correct name with different casing
    return location_clean.title()

async def _resolve_program_name(program: str) -> Optional[str]:
    """
    Resolve program name/description to standard program area name
    """
    if not program:
        return None
        
    # Standard program areas from our data
    standard_programs = [
        "Electric Programs",
        "Single Family Housing", 
        "Business Programs",
        "Multifamily Housing",
        "Telecommunications Programs",
        "Water and Environmental",
        "Community Facilities"
    ]
    
    # Program name mappings and variations
    program_mappings = {
        # Electric/Utility variations
        "electric": "Electric Programs",
        "electricity": "Electric Programs",
        "power": "Electric Programs",
        "utility": "Electric Programs",
        "utilities": "Electric Programs",
        "energy": "Electric Programs",
        
        # Housing variations  
        "housing": "Single Family Housing",  # Default to single family
        "single family": "Single Family Housing",
        "single-family": "Single Family Housing",
        "SFH": "Single Family Housing",
        "home": "Single Family Housing",
        "homes": "Single Family Housing",
        "residential": "Single Family Housing",
        "multifamily": "Multifamily Housing",
        "multi-family": "Multifamily Housing",
        "MFH": "Multifamily Housing",
        "apartment": "Multifamily Housing",
        "apartments": "Multifamily Housing",
        
        # Business variations
        "business": "Business Programs",
        "businesses": "Business Programs", 
        "commercial": "Business Programs",
        "enterprise": "Business Programs",
        "economic development": "Business Programs",
        
        # Telecommunications variations
        "telecom": "Telecommunications Programs",
        "telecommunications": "Telecommunications Programs", 
        "broadband": "Telecommunications Programs",
        "internet": "Telecommunications Programs",
        "connectivity": "Telecommunications Programs",
        "communication": "Telecommunications Programs",
        
        # Water/Environmental variations
        "water": "Water and Environmental",
        "environmental": "Water and Environmental",
        "wastewater": "Water and Environmental",
        "sewer": "Water and Environmental",
        "environment": "Water and Environmental",
        "clean water": "Water and Environmental",
        
        # Community variations
        "community": "Community Facilities",
        "facilities": "Community Facilities",
        "public": "Community Facilities"
    }
    
    # Clean and normalize input
    program_clean = program.strip()
    program_lower = program_clean.lower()
    
    # Check direct mapping first
    if program_lower in program_mappings:
        return program_mappings[program_lower]
    
    # Check if input already matches a standard program name (case-insensitive)
    for std_program in standard_programs:
        if program_clean.lower() == std_program.lower():
            return std_program
    
    # Check for partial matches in standard program names
    for std_program in standard_programs:
        if program_lower in std_program.lower() or std_program.lower() in program_lower:
            return std_program
    
    # If no match found, return the cleaned input
    # This allows the API to handle unknown program types
    return program_clean.title()


def _apply_smart_defaults(
    response_type: str,
    limit: int,
    filters: dict,
    total_matching: int = 0
) -> dict:
    """
    Apply smart defaults based on response type and context
    
    Returns adjusted parameters and recommendations
    """
    adjusted_params = {
        "limit": limit,
        "filters": filters.copy(),
        "recommendations": []
    }
    
    # Smart limiting based on response type
    if response_type == "summary":
        # For summaries, prefer smaller samples unless explicitly requested
        if limit > 50 and total_matching > 100:
            adjusted_params["limit"] = 20
            adjusted_params["recommendations"].append(
                f"Limited to {adjusted_params['limit']} records for summary analysis. Use response_type='details' for more records."
            )
    
    elif response_type == "details":
        # For details, ensure reasonable limits
        if limit > MAX_LIMIT:
            adjusted_params["limit"] = MAX_LIMIT
            adjusted_params["recommendations"].append(
                f"Limited to maximum {MAX_LIMIT} records. Use pagination for more data."
            )
        
        # Suggest aggregation for large result sets
        if total_matching > 500:
            adjusted_params["recommendations"].append(
                "Large result set detected. Consider using response_type='summary' for faster aggregated results."
            )
    
    # Smart filtering suggestions
    if not filters:
        adjusted_params["recommendations"].append(
            "No filters applied. Results may be very broad. Consider filtering by location, program, or fiscal_year."
        )
    
    if len(filters) == 1 and "fiscal_year" not in filters:
        adjusted_params["recommendations"].append(
            "Consider adding fiscal_year filter to get more recent or specific time period data."
        )
    
    return adjusted_params

def _format_response_with_context(
    data: List[dict],
    query_metadata: dict,
    smart_defaults: dict,
    additional_context: dict = None
) -> dict:
    """
    Format response with rich metadata and contextual information
    """
    formatted_response = {
        "query_metadata": query_metadata,
        "data": data,
        "response_context": {
            "smart_defaults_applied": smart_defaults.get("recommendations", []),
            "data_scope": f"{len(data)} records returned",
            "query_optimization_suggestions": []
        }
    }
    
    # Add additional context if provided
    if additional_context:
        formatted_response["response_context"].update(additional_context)
    
    # Calculate aggregations from data for LLM integration compatibility
    if len(data) > 0:
        investment_amounts = [record.get("investment_dollars_numeric", 0) for record in data if record.get("investment_dollars_numeric", 0) > 0]
        total_investments = sum(record.get("number_of_investments", 0) or 0 for record in data)
        total_dollars = sum(investment_amounts) if investment_amounts else 0
        avg_investment = total_dollars / len(investment_amounts) if investment_amounts else 0
        
        # Add aggregations to response context
        formatted_response["response_context"]["aggregations"] = {
            "total_investment_dollars": total_dollars,
            "total_investments": total_investments,
            "average_investment": avg_investment
        }
    
    # Add query optimization suggestions based on data patterns
    if len(data) > 0:
        # Check for common patterns in the data
        states = set(record.get("state_name") for record in data if record.get("state_name"))
        programs = set(record.get("program_area") for record in data if record.get("program_area"))
        
        if len(states) > 5:
            formatted_response["response_context"]["query_optimization_suggestions"].append(
                f"Data spans {len(states)} states. Consider filtering by specific state for more focused analysis."
            )
        
        if len(programs) > 3:
            formatted_response["response_context"]["query_optimization_suggestions"].append(
                f"Data includes {len(programs)} program areas. Consider filtering by specific program type."
            )
    
    return formatted_response

async def _get_data_freshness_info() -> dict:
    """
    Get data freshness information  checking how old the data are
    """
    try:
        summary = await api_client.get_data_summary()
        
        if not summary.get("last_updated"):
            return {
                "data_age_days": "unknown",
                "freshness_status": "unknown",
                "last_update": None,
                "recommendation": "Unable to determine data age. Consider running refresh_data() to ensure fresh data."
            }
        
        processed_at = datetime.fromisoformat(summary["last_updated"])
        data_age = datetime.now() - processed_at
        data_age_days = data_age.days
        
        # Determine freshness status
        if data_age_days < 8:
            freshness_status = "very_fresh"
            recommendation = "Data is from this week."
        elif data_age_days <= 14:
            freshness_status = "fresh"
            recommendation = "Data is recent."
        elif data_age_days <= 21:
            freshness_status = "acceptable"
            recommendation = "Data is from within this month."
        elif data_age_days <= 27:
            freshness_status = "getting_stale"
            recommendation = "Consider asking user if they want to refresh data for the latest information."
        else:
            freshness_status = "stale"
            recommendation = "Data is outdated. Consider asking user if they want to run refresh_data() for current information."
        
        return {
            "data_age_days": data_age_days,
            "freshness_status": freshness_status,
            "last_update": summary["last_updated"],
            "recommendation": recommendation
        }
        
    except Exception as e:
        return {
            "data_age_days": "error",
            "freshness_status": "unknown",
            "last_update": None,
            "recommendation": f"Error checking data age: {str(e)}"
        }

@mcp.tool()
async def get_data_info() -> dict:
    """
    Get metadata about the Local USDA Rural Investment dataset
    
    Returns information about data freshness, available fields, dataset statistics,
    and information about both detailed and summary data sources
    """
    try:
        if not await api_client.health_check():
            return {
                "error": "Local USDA data API is not available",
                "status": "api_unavailable"
            }
        
        summary = await api_client.get_data_summary()
        freshness_info = await _get_data_freshness_info()
        
        # Extract data from enhanced summary
        investments_table = summary.get("investments_table", {})
        summary_table = summary.get("summary_table", {})
        
        # Get sample data to determine available states and programs
        sample_result = await api_client.query_investments_data({}, 100, 0)
        sample_data = sample_result.get("data", [])
        
        # Extract unique values from sample data
        states = sorted(list(set(record.get("state_name") for record in sample_data if record.get("state_name"))))
        program_areas = sorted(list(set(record.get("program_area") for record in sample_data if record.get("program_area"))))
        
        return {
            "dataset_overview": {
                "name": "Enhanced USDA Rural Development Investment Data",
                "dual_datasets": {
                    "detailed_transactions": {
                        "records": investments_table.get("record_count", 0),
                        "description": "Individual transaction-level data",
                        "fiscal_year_range": investments_table.get("fiscal_year_range", "Unknown")
                    },
                    "historical_summary": {
                        "records": summary_table.get("record_count", 0),
                        "description": "State+program aggregated data",
                        "fiscal_year_range": summary_table.get("fiscal_year_range", "Unknown")
                    }
                },
                "last_updated": summary.get("last_updated", "Unknown"),
                "geographic_coverage": "All 50 US States",
                "data_source": "USDA Rural Development Rural Data Gateway"
            },
            "available_filters": {
                "states": states,
                "program_areas": program_areas,
                "fiscal_years": summary_table.get("fiscal_year_range", "Unknown")
            },
            "data_structure": {
                "detailed_data_fields": [
                    "fiscal_year", "state_name", "county", "program_area", "program",
                    "investment_dollars_numeric", "borrower_name", "city", "project_name",
                    "lender_name", "naics_industry_sector", "zip_code"
                ],
                "summary_data_fields": [
                    "fiscal_year", "state_name", "program_area", "investment_dollars_numeric",
                    "number_of_investments"
                ],
                "geographic_detail": "State and County level (detailed data only)",
                "temporal_detail": "Fiscal year (October-September)"
            },
            "data_freshness": freshness_info,
            "usage_examples": [
                {
                    "description": "Get Texas investment summary from aggregated data",
                    "tool_call": "get_rural_data(location='Texas', response_type='summary')"
                },
                {
                    "description": "Get detailed transaction data for broadband programs", 
                    "tool_call": "get_rural_data(program='broadband', response_type='details', data_source='detailed')"
                },
                {
                    "description": "Get historical summary data spanning multiple years",
                    "tool_call": "get_summary_data(location='California')"
                },
                {
                    "description": "Compare top 3 states using aggregated data",
                    "tool_call": "compare_data(comparison_type='regions', locations=['TX', 'CA', 'FL'], metric='total_dollars')"
                }
            ]
        }
    
    except Exception as e:
        return {
            "error": f"Error getting data info: {str(e)}",
            "status": "info_error"
        }

if __name__ == "__main__":
    print("Starting USDA Rural Data Gateway MCP Server...")
    print("This server provides LLM access to USDA Rural Investment data")
    
    # Run the MCP server
    mcp.run()