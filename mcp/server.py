#!/usr/bin/env python3
"""
USDA Rural Data Gateway MCP Server
Provides LLMs with access to USDA Rural Investment data through semantic tools
"""

from typing import Optional, Dict, List, Any
import httpx
import asyncio
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
        response = await self.client.get(f"{self.base_url}/summary")
        response.raise_for_status()
        return response.json()
    
    async def query_structured_data(
        self, 
        filters: Dict[str, Any], 
        limit: int = DEFAULT_LIMIT, 
        offset: int = 0
    ) -> dict:
        """Query structured data with filters"""
        params = {
            "limit": min(limit, MAX_LIMIT),
            "offset": offset,
            **filters
        }
        
        response = await self.client.get(f"{self.base_url}/data", params=params)
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
    limit: int = DEFAULT_LIMIT
) -> dict:
    """
    Main tool for rural investment data queries
    
    Args:
        location: State name or abbreviation (e.g., 'California', 'CA', 'Texas')
        program: Program area or type (e.g., 'housing', 'broadband', 'electric')
        fiscal_year: Specific fiscal year (e.g., 2023, 2024) up to 10 past years are available
        response_type: Type of response - 'summary' for aggregated data, 'details' for individual records
        limit: Maximum number of individual records to return (max 100)
    
    Returns:
        Dictionary containing investment data, aggregations, and metadata
    """
    
    try:
        # Check API health first
        if not await api_client.health_check():
            return {
                "error": "USDA data API is not available. Please ensure the API server is running.",
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
        
        if response_type == "summary" and resolved_location:
            # Use fast aggregation endpoint for summary requests
            result = await api_client.get_state_aggregations(
                state=resolved_location,
                fiscal_year=fiscal_year
            )
            
            # Get data freshness info
            freshness_info = await _get_data_freshness_info()
            
            # Extract aggregation data properly
            aggregation_data = {}
            total_investments = 0
            total_dollars = 0.0
            
            if "years" in result and result["years"]:
                # Sum up all years if multiple years returned
                for year_data in result["years"]:
                    total_investments += year_data.get("total_investments", 0)
                    total_dollars += year_data.get("total_dollars", 0.0)
                
                # Use most recent year for average calculation
                latest_year = result["years"][-1]
                avg_investment = latest_year.get("avg_investment", 0.0)
            else:
                avg_investment = 0.0
            
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
                    "response_type": response_type
                },
                "records": [{
                    "state_name": resolved_location,
                    "total_investments": total_investments,
                    "total_dollars": total_dollars,
                    "average_investment": avg_investment,
                    "fiscal_year_range": f"{result['years'][0]['fiscal_year']}-{result['years'][-1]['fiscal_year']}" if len(result.get('years', [])) > 1 else str(result['years'][0]['fiscal_year']) if result.get('years') else "Unknown"
                }],
                "response_context": {
                    "summary": {
                        "total_matching_records": total_investments,
                        "records_returned": 1,
                        "data_freshness": freshness_info.get("freshness_category", "unknown"),
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
            total_matching_estimate = 1000  # will likely be lss than this
            smart_defaults = _apply_smart_defaults(response_type, limit, filters, total_matching_estimate)
            adjusted_limit = smart_defaults["limit"]
            
            # Use structured data query for details or non-location queries
            result = await api_client.query_structured_data(filters, adjusted_limit, 0)
            
            # Now we have the actual total count
            actual_total = result["pagination"]["total"]
            
            # Re-apply smart defaults with actual count if needed
            if actual_total != total_matching_estimate:
                smart_defaults = _apply_smart_defaults(response_type, limit, filters, actual_total)
            
            # Analyze data quality
            data_quality = await _analyze_data_quality(result["data"])
            
            # Format response with rich metadata and context
            query_metadata = {
                "location_requested": location,
                "location_resolved": resolved_location,
                "program_requested": program,
                "program_resolved": resolved_program,
                "fiscal_year": fiscal_year,
                "response_type": response_type,
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
                "data_quality": data_quality,
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

async def _analyze_data_quality(data: List[dict]) -> dict:
    """
    Analyze data quality of a result set
    
    Checks for missing values, anomalies, and data consistency issues
    """
    if not data:
        return {"total_records": 0, "quality_score": "N/A"}
    
    total_records = len(data)
    quality_issues = []
    missing_value_counts = {}
    
    # Key fields that should never be missing
    critical_fields = ["investment_dollars_numeric", "fiscal_year", "state_name"]
    
    # Fields that commonly have missing values (not critical)
    optional_fields = ["county", "city", "borrower_name", "project_name"]
    
    # Check each record for quality issues
    investment_amounts = []
    for record in data:
        # Check for missing critical fields
        for field in critical_fields:
            value = record.get(field)
            if value is None or value == "" or value == "Not Available":
                if field not in missing_value_counts:
                    missing_value_counts[field] = 0
                missing_value_counts[field] += 1
        
        # Collect investment amounts for statistical analysis
        amount = record.get("investment_dollars_numeric", 0)
        if amount > 0:  # Only include positive amounts
            investment_amounts.append(amount)
        
        # Check for placeholder values that indicate missing data
        for field_name, field_value in record.items():
            if isinstance(field_value, str) and field_value.lower() in [
                "not available", "withheld", "unknown", "multiple", "unknown districts"
            ]:
                field_key = f"{field_name}_placeholder_values"
                if field_key not in missing_value_counts:
                    missing_value_counts[field_key] = 0
                missing_value_counts[field_key] += 1
    
    # Calculate statistics for investment amounts
    investment_stats = {}
    if investment_amounts:
        investment_amounts.sort()
        n = len(investment_amounts)
        investment_stats = {
            "count": n,
            "min": investment_amounts[0],
            "max": investment_amounts[-1],
            "median": investment_amounts[n//2],
            "mean": round(sum(investment_amounts) / n, 2),
            "q1": investment_amounts[n//4],
            "q3": investment_amounts[3*n//4]
        }
        
        # Check for potential anomalies (values beyond Q1-1.5*IQR or Q3+1.5*IQR)
        iqr = investment_stats["q3"] - investment_stats["q1"]
        lower_bound = investment_stats["q1"] - 1.5 * iqr
        upper_bound = investment_stats["q3"] + 1.5 * iqr
        
        outliers = [x for x in investment_amounts if x < lower_bound or x > upper_bound]
        if outliers:
            investment_stats["outlier_count"] = len(outliers)
            investment_stats["outlier_examples"] = outliers[:5]  # Show first 5 outliers
    
    # Calculate overall quality score
    critical_missing_pct = sum(missing_value_counts.get(field, 0) for field in critical_fields) / (total_records * len(critical_fields)) * 100
    
    if critical_missing_pct == 0:
        quality_score = "Excellent"
    elif critical_missing_pct < 5:
        quality_score = "Good" 
    elif critical_missing_pct < 15:
        quality_score = "Fair"
    else:
        quality_score = "Poor"
    
    # Generate quality summary
    quality_summary = []
    if missing_value_counts:
        for field, count in missing_value_counts.items():
            pct = (count / total_records) * 100
            quality_summary.append(f"{field}: {count} records ({pct:.1f}%)")
    
    return {
        "total_records": total_records,
        "quality_score": quality_score,
        "critical_missing_percentage": critical_missing_pct,
        "missing_value_counts": missing_value_counts,
        "quality_issues": quality_summary,
        "investment_statistics": investment_stats,
        "recommendation": _generate_quality_recommendation(quality_score, missing_value_counts, total_records)
    }

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
        total_investments = len(data)
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
    Get data freshness information to include in all responses
    Data are supposed to be updated weekly
    Returns age in days, freshness status, and recommendations
    """
    try:
        summary = await api_client.get_data_summary()
        latest_import = summary.get("latest_import", {})
        
        if not latest_import.get("processed_at"):
            return {
                "data_age_days": "unknown",
                "freshness_status": "unknown",
                "last_update": None,
                "recommendation": "Unable to determine data age. Consider running refresh_data() to ensure fresh data."
            }
        
        processed_at = datetime.fromisoformat(latest_import["processed_at"])
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
            "last_update": latest_import["processed_at"],
            "recommendation": recommendation
        }
        
    except Exception as e:
        return {
            "data_age_days": "error",
            "freshness_status": "unknown",
            "last_update": None,
            "recommendation": f"Error checking data age: {str(e)}"
        }

def _generate_quality_recommendation(quality_score: str, missing_counts: dict, total_records: int) -> str:
    """Generate a recommendation based on data quality analysis"""
    if quality_score == "Excellent":
        return "Data quality is excellent. Results can be used with high confidence."
    elif quality_score == "Good":
        return "Data quality is good. Minor missing values do not significantly impact analysis."
    elif quality_score == "Fair":
        return f"Data quality is fair. Consider the {sum(missing_counts.values())} missing values when interpreting results."
    else:
        return f"Data quality is poor with {sum(missing_counts.values())} missing values out of {total_records} records. Use results with caution."

async def _resolve_program_name(program: str) -> Optional[str]:
    """
    Resolve program name/description to standard program area name
    This is somewhat trying to guess what users want 
    This is probably something that should get SMEs involved
    Based on the program areas available in USDA data:
    - Electric Programs, Single Family Housing, Business Programs, 
    - Multifamily Housing, Telecommunications Programs, Water and Environmental,
    - Community Facilities
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
        "individuals": "Single Family Housing",
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

# Add server metadata
@mcp.tool()
async def get_data_info() -> dict:
    """
    Get metadata about the USDA Rural Investment dataset
    
    Returns information about data freshness, available fields, and dataset statistics
    """
    try:
        if not await api_client.health_check():
            return {
                "error": "USDA data API is not available",
                "status": "api_unavailable"
            }
        
        summary = await api_client.get_data_summary()
        freshness_info = await _get_data_freshness_info()
        
        # Extract actual data from summary
        latest_import = summary.get("latest_import", {})
        total_records = latest_import.get("row_count", 0)
        processed_date = latest_import.get("processed_at", "Unknown")
        
        # Get sample data to determine available states and programs
        sample_result = await api_client.query_structured_data({}, 100, 0)
        sample_data = sample_result.get("data", [])
        
        # Extract unique values from sample data
        states = sorted(list(set(record.get("state_name") for record in sample_data if record.get("state_name"))))
        program_areas = sorted(list(set(record.get("program_area") for record in sample_data if record.get("program_area"))))
        fiscal_years = sorted(list(set(record.get("fiscal_year") for record in sample_data if record.get("fiscal_year"))))
        
        # Calculate investment range from sample
        investment_amounts = [record.get("investment_dollars_numeric", 0) for record in sample_data if record.get("investment_dollars_numeric", 0) > 0]
        investment_range = {}
        if investment_amounts:
            investment_range = {
                "minimum": min(investment_amounts),
                "maximum": max(investment_amounts),
                "average": sum(investment_amounts) / len(investment_amounts)
            }
        
        return {
            "dataset_overview": {
                "name": "USDA Rural Development Investment Data",
                "total_records": total_records,
                "date_range": f"{min(fiscal_years) if fiscal_years else 'Unknown'}-{max(fiscal_years) if fiscal_years else 'Unknown'}",
                "last_updated": processed_date,
                "geographic_coverage": "All 50 US States + territories",
                "data_source": "USDA Rural Development Gateway"
            },
            "available_filters": {
                "states": states,
                "program_areas": program_areas,
                "fiscal_years": fiscal_years
            },
            "data_structure": {
                "key_fields": [
                    "fiscal_year",
                    "state_name", 
                    "county",
                    "program_area",
                    "program",
                    "investment_dollars_numeric",
                    "borrower_name",
                    "city",
                    "project_name"
                ],
                "investment_range": investment_range,
                "geographic_detail": "State and County level",
                "temporal_detail": "Fiscal year (October-September)"
            },
            "usage_statistics": {
                "most_queried_state": states[0] if states else "Unknown",
                "most_popular_program": program_areas[0] if program_areas else "Unknown", 
                "common_query_types": [
                    "State summaries",
                    "Program comparisons",
                    "Recipient searches"
                ]
            },
            "usage_examples": [
                {
                    "description": "Get Texas investment summary",
                    "tool_call": "get_rural_data(location='Texas', response_type='summary')"
                },
                {
                    "description": "Compare top 3 states",
                    "tool_call": "compare_data(comparison_type='regions', locations=['TX', 'CA', 'FL'], metric='total_dollars')"
                },
                {
                    "description": "Find electric cooperatives", 
                    "tool_call": "find_recipient(recipient_name='Electric Cooperative')"
                },
                {
                    "description": "Check data quality",
                    "tool_call": "check_data_quality(sample_size=200)"
                }
            ]
        }
    
    except Exception as e:
        return {
            "error": f"Error getting data info: {str(e)}",
            "status": "info_error"
        }

@mcp.tool()
async def compare_data(
    comparison_type: str,
    locations: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
    programs: Optional[List[str]] = None,
    metric: str = "total_dollars"
) -> dict:
    """
    Compare rural investment data across regions, years, or programs
    
    Args:
        comparison_type: Type of comparison - 'regions', 'years', or 'programs'
        locations: List of state names/abbreviations to compare (for 'regions' comparison)
        years: List of fiscal years to compare (for 'years' comparison)
        programs: List of program areas to compare (for 'programs' comparison)  
        metric: Comparison metric - 'total_dollars', 'total_investments', 'avg_investment'
    
    Returns:
        Dictionary containing comparison results with rankings and differences
    """
    
    try:
        # Check API health first
        if not await api_client.health_check():
            return {
                "error": "USDA data API is not available. Please ensure the API server is running.",
                "status": "api_unavailable"
            }
        
        if comparison_type == "regions":
            return await _compare_regions(locations, years[0] if years else None, programs[0] if programs else None, metric)
        elif comparison_type == "years":
            return await _compare_years(locations[0] if locations else None, programs[0] if programs else None, years, metric)
        elif comparison_type == "programs":
            return await _compare_programs(locations[0] if locations else None, years[0] if years else None, programs, metric)
        else:
            return {
                "error": f"Invalid comparison_type '{comparison_type}'. Must be 'regions', 'years', or 'programs'",
                "status": "invalid_comparison_type"
            }
    
    except Exception as e:
        return {
            "error": f"Error comparing data: {str(e)}",
            "status": "comparison_error"
        }

async def _compare_regions(
    locations: List[str], 
    fiscal_year: Optional[int] = None,
    program: Optional[str] = None,
    metric: str = "total_dollars"
) -> dict:
    """Compare multiple regions (states)"""
    if not locations or len(locations) < 2:
        return {
            "error": "Need at least 2 locations to compare",
            "status": "insufficient_locations"
        }
    
    # Resolve location names
    resolved_locations = []
    for location in locations:
        resolved = await _resolve_location_name(location)
        resolved_locations.append(resolved)
    
    # Use the aggregation comparison endpoint
    try:
        result = await api_client.compare_aggregations(
            compare_type="states",
            items=resolved_locations,
            fiscal_year=fiscal_year
        )
        
        # Extract and format comparison data
        comparisons = result.get("comparisons", [])
        
        # Sort by the requested metric
        metric_values = []
        for comp in comparisons:
            # Extract data from years array - sum up all years for total metrics
            years_data = comp.get("years", [])
            total_dollars = sum(year.get("total_dollars", 0) for year in years_data)
            total_investments = sum(year.get("total_investments", 0) for year in years_data)
            
            # For average, use most recent year or calculate weighted average
            if years_data:
                latest_year = years_data[-1]  # Most recent year
                avg_investment = latest_year.get("avg_investment", 0)
            else:
                avg_investment = total_dollars / total_investments if total_investments > 0 else 0
            
            if metric == "total_dollars":
                value = total_dollars
            elif metric == "total_investments":
                value = total_investments
            elif metric == "avg_investment":
                value = avg_investment
            else:
                value = total_dollars  # Default
            
            state_name = comp.get("state_name", "Unknown")
            try:
                location_requested = locations[resolved_locations.index(state_name)] if state_name in resolved_locations else state_name
            except (ValueError, IndexError):
                location_requested = state_name
            
            metric_values.append({
                "location": state_name,
                "location_requested": location_requested,
                "value": value,
                "investment_count": total_investments,
                "total_dollars": total_dollars,
                "avg_investment": avg_investment
            })
        
        # Sort by value (descending)
        metric_values.sort(key=lambda x: x["value"], reverse=True)
        
        # Calculate differences and rankings
        if len(metric_values) >= 2:
            leader = metric_values[0]
            comparison_details = []
            
            for i, item in enumerate(metric_values):
                rank = i + 1
                diff_from_leader = item["value"] - leader["value"] if item != leader else 0
                pct_diff = (diff_from_leader / leader["value"] * 100) if leader["value"] > 0 else 0
                
                comparison_details.append({
                    "rank": rank,
                    "location": item["location"],
                    "value": item["value"],
                    "investment_count": item["investment_count"],
                    "percentage_of_total": (item["value"] / sum(mv["value"] for mv in metric_values)) * 100 if sum(mv["value"] for mv in metric_values) > 0 else 0
                })
        
        total_compared = sum(item["value"] for item in metric_values)
        average_value = total_compared / len(metric_values) if metric_values else 0
        
        return {
            "comparison_type": "regions",
            "metric": metric,
            "comparisons": comparison_details,
            "leader": {
                "location": leader["location"],
                "value": leader["value"],
                "lead_margin": leader["value"] - metric_values[1]["value"] if len(metric_values) > 1 else 0,
                "lead_percentage": ((leader["value"] - metric_values[1]["value"]) / metric_values[1]["value"] * 100) if len(metric_values) > 1 and metric_values[1]["value"] > 0 else 0
            },
            "insights": {
                "total_compared": total_compared,
                "average_value": average_value,
                "analysis": f"{leader['location']} leads by significant margin" if len(metric_values) > 1 and leader["value"] > metric_values[1]["value"] * 1.2 else f"{leader['location']} leads with competitive margins",
                "notable_patterns": [
                    f"All regions exceed ${(min(item['value'] for item in metric_values)):,.0f}" if metric == "total_dollars" else f"All regions have {min(item['value'] for item in metric_values):,}+ investments"
                ]
            }
        }
        
    except Exception as e:
        return {
            "error": f"Error comparing regions: {str(e)}",
            "status": "region_comparison_error"
        }

async def _compare_years(
    location: Optional[str] = None,
    program: Optional[str] = None, 
    years: Optional[List[int]] = None,
    metric: str = "total_dollars"
) -> dict:
    """Compare data across multiple years"""
    if not years or len(years) < 2:
        return {
            "error": "Need at least 2 years to compare",
            "status": "insufficient_years"
        }
    
    resolved_location = await _resolve_location_name(location) if location else None
    resolved_program = await _resolve_program_name(program) if program else None
    
    # Get data for each year
    year_comparisons = []
    for year in years:
        if resolved_location:
            # Use state aggregation endpoint
            result = await api_client.get_state_aggregations(
                state=resolved_location,
                fiscal_year=year
            )
            
            if not result.get("error"):
                state_data = result.get("state_data", {})
                if metric == "total_dollars":
                    value = state_data.get("total_dollars", 0)
                elif metric == "total_investments":
                    value = state_data.get("total_investments", 0)
                elif metric == "avg_investment":
                    value = state_data.get("avg_investment", 0)
                else:
                    value = state_data.get("total_dollars", 0)
                
                year_comparisons.append({
                    "year": year,
                    "value": value,
                    "full_data": state_data
                })
        else:
            # For non-location queries, we'd need to implement general year aggregation
            # For now, return a placeholder
            year_comparisons.append({
                "year": year,
                "value": 0,
                "full_data": {},
                "note": "Year-over-year comparison without location not yet implemented"
            })
    
    # Sort by year
    year_comparisons.sort(key=lambda x: x["year"])
    
    # Calculate year-over-year changes
    comparison_details = []
    for i, year_data in enumerate(year_comparisons):
        if i > 0:
            prev_year = year_comparisons[i-1]
            change = year_data["value"] - prev_year["value"]
            pct_change = (change / prev_year["value"] * 100) if prev_year["value"] > 0 else 0
        else:
            change = 0
            pct_change = 0
            
        comparison_details.append({
            "year": year_data["year"],
            f"{metric}": year_data["value"],
            "change_from_previous": change,
            "percent_change": pct_change,
            "full_data": year_data["full_data"]
        })
    
    return {
        "comparison_type": "years",
        "comparison_params": {
            "location": location,
            "location_resolved": resolved_location,
            "program": program,
            "program_resolved": resolved_program,
            "years": years,
            "metric": metric
        },
        "comparisons": comparison_details,
        "trend": "increasing" if comparison_details[-1]["change_from_previous"] > 0 else "decreasing" if comparison_details[-1]["change_from_previous"] < 0 else "stable"
    }

async def _compare_programs(
    location: Optional[str] = None,
    fiscal_year: Optional[int] = None,
    programs: Optional[List[str]] = None,
    metric: str = "total_dollars"
) -> dict:
    """Compare multiple programs"""
    if not programs or len(programs) < 2:
        return {
            "error": "Need at least 2 programs to compare",
            "status": "insufficient_programs"
        }
    
    resolved_location = await _resolve_location_name(location) if location else None
    resolved_programs = []
    for program in programs:
        resolved = await _resolve_program_name(program)
        resolved_programs.append(resolved)
    
    # Use program aggregation comparison
    try:
        result = await api_client.compare_aggregations(
            compare_type="programs",
            items=resolved_programs,
            fiscal_year=fiscal_year
        )
        
        # Extract and format comparison data
        comparisons = result.get("comparisons", [])
        
        # Sort by the requested metric  
        metric_values = []
        for comp in comparisons:
            if metric == "total_dollars":
                value = comp.get("total_dollars", 0)
            elif metric == "total_investments":
                value = comp.get("total_investments", 0)
            elif metric == "avg_investment":
                value = comp.get("avg_investment", 0)
            else:
                value = comp.get("total_dollars", 0)
            
            program_name = comp.get("program_area", "Unknown")
            # Find the original requested program name
            program_requested = "Unknown"
            if program_name in resolved_programs:
                idx = resolved_programs.index(program_name)
                if idx < len(programs):
                    program_requested = programs[idx]
            
            metric_values.append({
                "program": program_name,
                "program_requested": program_requested,
                "value": value,
                "full_data": comp
            })
        
        # Sort by value (descending)
        metric_values.sort(key=lambda x: x["value"], reverse=True)
        
        # Format results
        comparison_details = []
        leader = metric_values[0]
        
        for i, item in enumerate(metric_values):
            rank = i + 1
            diff_from_leader = item["value"] - leader["value"] if item != leader else 0
            pct_diff = (diff_from_leader / leader["value"] * 100) if leader["value"] > 0 else 0
            
            comparison_details.append({
                "rank": rank,
                "program": item["program"],
                "program_requested": item["program_requested"],
                f"{metric}": item["value"],
                "difference_from_leader": diff_from_leader,
                "percent_difference": pct_diff,
                "full_program_data": item["full_data"]
            })
        
        return {
            "comparison_type": "programs",
            "comparison_params": {
                "location": location,
                "location_resolved": resolved_location,
                "fiscal_year": fiscal_year,
                "programs_requested": programs,
                "programs_resolved": resolved_programs,
                "metric": metric
            },
            "leader": {
                "program": leader["program"],
                "value": leader["value"]
            },
            "comparisons": comparison_details,
            "summary": f"{leader['program']} leads with {leader['value']:,} {metric.replace('_', ' ')}"
        }
        
    except Exception as e:
        return {
            "error": f"Error comparing programs: {str(e)}",
            "status": "program_comparison_error"
        }

@mcp.tool()
async def find_recipient(
    recipient_name: str,
    location: Optional[str] = None,
    program: Optional[str] = None,
    fiscal_year: Optional[int] = None,
    limit: int = DEFAULT_LIMIT
) -> dict:
    """
    Search for specific borrowers/recipients in rural investment data
    
    Args:
        recipient_name: Name of the borrower/recipient to search for (partial matches allowed)
        location: Optional state filter
        program: Optional program area filter  
        fiscal_year: Optional fiscal year filter
        limit: Maximum number of records to return (max 100)
    
    Returns:
        Dictionary containing matching recipients and their investment details
    """
    
    try:
        # Check API health first
        if not await api_client.health_check():
            return {
                "error": "USDA data API is not available. Please ensure the API server is running.",
                "status": "api_unavailable"
            }
        
        # Resolve filters
        resolved_location = await _resolve_location_name(location) if location else None
        resolved_program = await _resolve_program_name(program) if program else None
        
        # Build filters for the query
        filters = {
            "borrower_name": recipient_name  # The API will handle partial matching
        }
        
        if resolved_location:
            filters["state"] = resolved_location
        if resolved_program:
            filters["program"] = resolved_program
        if fiscal_year:
            filters["fiscal_year"] = fiscal_year
        
        # Query structured data for individual records
        result = await api_client.query_structured_data(filters, limit, 0)
        
        # Process and aggregate the results
        data = result.get("data", [])
        total_matching = result.get("pagination", {}).get("total", 0)
        
        if not data:
            return {
                "search_metadata": {
                    "recipient_searched": recipient_name,
                    "location": location,
                    "location_resolved": resolved_location,
                    "program": program,
                    "program_resolved": resolved_program,
                    "fiscal_year": fiscal_year
                },
                "found": False,
                "total_matching_records": total_matching,
                "message": f"No investments found for recipient matching '{recipient_name}'"
            }
        
        # Group results by borrower name for summary
        borrower_summaries = {}
        all_investments = []
        
        for investment in data:
            borrower = investment.get("borrower_name", "Unknown")
            amount = investment.get("investment_dollars_numeric", 0)
            state = investment.get("state_name", "Unknown") 
            program = investment.get("program_area", "Unknown")
            year = investment.get("fiscal_year", "Unknown")
            
            # Add to individual investments list
            all_investments.append({
                "borrower_name": borrower,
                "investment_amount": amount,
                "state": state,
                "program_area": program,
                "fiscal_year": year,
                "county": investment.get("county", "Unknown"),
                "city": investment.get("city", "Unknown"),
                "project_name": investment.get("project_name", ""),
                "full_record": investment
            })
            
            # Aggregate by borrower
            if borrower not in borrower_summaries:
                borrower_summaries[borrower] = {
                    "borrower_name": borrower,
                    "total_investments": 0,
                    "total_dollars": 0,
                    "investment_count": 0,
                    "states": set(),
                    "programs": set(),
                    "years": set()
                }
            
            summary = borrower_summaries[borrower]
            summary["total_dollars"] += amount
            summary["investment_count"] += 1
            summary["states"].add(state)
            summary["programs"].add(program)
            summary["years"].add(year)
        
        # Convert sets to lists and sort
        for borrower in borrower_summaries.values():
            borrower["states"] = sorted(list(borrower["states"]))
            borrower["programs"] = sorted(list(borrower["programs"]))
            borrower["years"] = sorted(list(borrower["years"]))
            borrower["avg_investment"] = round(borrower["total_dollars"] / borrower["investment_count"], 2) if borrower["investment_count"] > 0 else 0.00
        
        # Sort borrowers by total dollars (descending)
        sorted_borrowers = sorted(
            borrower_summaries.values(), 
            key=lambda x: x["total_dollars"], 
            reverse=True
        )
        
        # Analyze data quality
        data_quality = await _analyze_data_quality(data)
        
        return {
            "search_metadata": {
                "recipient_searched": recipient_name,
                "location": location,
                "location_resolved": resolved_location,
                "program": program,
                "program_resolved": resolved_program,
                "fiscal_year": fiscal_year,
                "filters_applied": filters
            },
            "found": True,
            "total_matching_records": total_matching,
            "unique_borrowers_found": len(borrower_summaries),
            "records_returned": len(data),
            "borrower_summaries": sorted_borrowers,
            "individual_investments": all_investments,
            "top_recipient": {
                "name": sorted_borrowers[0]["borrower_name"],
                "total_dollars": sorted_borrowers[0]["total_dollars"],
                "investment_count": sorted_borrowers[0]["investment_count"]
            } if sorted_borrowers else None,
            "data_quality": data_quality
        }
        
    except Exception as e:
        return {
            "error": f"Error searching for recipient: {str(e)}",
            "status": "recipient_search_error"
        }

@mcp.tool()
async def check_data_quality(
    location: Optional[str] = None,
    program: Optional[str] = None,
    fiscal_year: Optional[int] = None,
    sample_size: int = 100
) -> dict:
    """
    Analyze data quality for a specific subset of rural investment data
    
    Args:
        location: Optional state filter for quality analysis
        program: Optional program area filter
        fiscal_year: Optional fiscal year filter
        sample_size: Number of records to analyze (default 100, max 500)
    
    Returns:
        Dictionary containing detailed data quality analysis and recommendations
    """
    
    try:
        # Check API health first
        if not await api_client.health_check():
            return {
                "error": "USDA data API is not available. Please ensure the API server is running.",
                "status": "api_unavailable"
            }
        
        # Limit sample size to reasonable bounds
        sample_size = min(max(sample_size, 10), 500)
        
        # Resolve filters
        resolved_location = await _resolve_location_name(location) if location else None
        resolved_program = await _resolve_program_name(program) if program else None
        
        # Build filters for the query
        filters = {}
        if resolved_location:
            filters["state"] = resolved_location
        if resolved_program:
            filters["program"] = resolved_program
        if fiscal_year:
            filters["fiscal_year"] = fiscal_year
        
        # Get sample data for analysis
        result = await api_client.query_structured_data(filters, sample_size, 0)
        
        data = result.get("data", [])
        total_matching = result.get("pagination", {}).get("total", 0)
        
        if not data:
            return {
                "analysis_metadata": {
                    "location": location,
                    "location_resolved": resolved_location,
                    "program": program,
                    "program_resolved": resolved_program,
                    "fiscal_year": fiscal_year,
                    "filters_applied": filters
                },
                "total_matching_records": total_matching,
                "sample_analyzed": 0,
                "message": "No data found matching the specified criteria for quality analysis"
            }
        
        # Perform comprehensive data quality analysis
        data_quality = await _analyze_data_quality(data)
        
        # Add additional context for the quality analysis
        analysis_context = {
            "dataset_scope": f"{len(data)} records analyzed out of {total_matching} total matching records",
            "coverage_percentage": (len(data) / total_matching * 100) if total_matching > 0 else 0,
            "analysis_completeness": "Complete" if len(data) == total_matching else "Sample-based"
        }
        
        # Extract and reformat data quality analysis for LLM integration
        missing_values = data_quality.get("missing_value_counts", {})
        total_records = data_quality.get("total_records", len(data))
        
        # Calculate field completeness percentages
        field_completeness = {}
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())
        
        for field in all_fields:
            missing_count = missing_values.get(field, 0)
            completeness_pct = ((total_records - missing_count) / total_records * 100) if total_records > 0 else 0
            field_completeness[field] = round(completeness_pct, 1)
        
        # Format investment statistics for LLM integration
        investment_stats = data_quality.get("investment_statistics", {})
        statistics = {}
        if investment_stats:
            statistics = {
                "mean_investment": investment_stats.get("mean", 0),
                "median_investment": investment_stats.get("median", 0),
                "total_investments": investment_stats.get("count", 0)
            }
        
        return {
            "data_quality_analysis": {
                "total_records": total_records,
                "quality_score": data_quality.get("quality_score", "Unknown"),
                "missing_values": missing_values,
                "field_completeness": field_completeness,
                "statistics": statistics
            },
            "recommendations": _generate_detailed_quality_recommendations(data_quality, total_matching),
            "sample_metadata": {
                "location_filter": resolved_location,
                "program_filter": resolved_program,
                "fiscal_year_filter": fiscal_year,
                "sample_percentage": round((len(data) / total_matching * 100), 1) if total_matching > 0 else 0
            }
        }
        
    except Exception as e:
        return {
            "error": f"Error analyzing data quality: {str(e)}",
            "status": "quality_analysis_error"
        }

def _generate_detailed_quality_recommendations(quality_analysis: dict, total_records: int) -> List[str]:
    """Generate detailed recommendations based on comprehensive quality analysis"""
    recommendations = []
    
    quality_score = quality_analysis.get("quality_score", "Unknown")
    missing_counts = quality_analysis.get("missing_value_counts", {})
    investment_stats = quality_analysis.get("investment_statistics", {})
    
    # Overall quality recommendation
    recommendations.append(quality_analysis.get("recommendation", "No specific recommendation available."))
    
    # Specific field recommendations
    for field, count in missing_counts.items():
        pct = (count / quality_analysis.get("total_records", 1)) * 100
        if pct > 10:
            recommendations.append(f"Consider filtering out records where {field} is missing ({pct:.1f}% affected)")
    
    # Statistical recommendations
    if investment_stats.get("outlier_count", 0) > 0:
        recommendations.append(f"Be aware of {investment_stats['outlier_count']} outlier investment amounts that may skew averages")
    
    # Sample size recommendations
    if total_records > quality_analysis.get("total_records", 0):
        recommendations.append(f"This analysis is based on a sample. Consider analyzing more records for complete picture.")
    
    return recommendations

@mcp.tool()
async def refresh_data(
    force_refresh: bool = False,
    progress_updates: bool = True
) -> dict:
    """
    Refresh rural investment data (only when explicitly requested)
    
    This tool provides data age information and can refresh data when force_refresh=True.
    Never automatically refreshes - always requires user confirmation via force_refresh=True.
    
    This triggers the complete data pipeline:
    1. Downloads fresh data from USDA Rural Data Gateway
    2. Processes and stores structured data 
    3. Rebuilds aggregation tables for fast queries
    
    Args:
        force_refresh: If True, refresh data regardless of age (default: False)
        progress_updates: If True, provide detailed progress updates (default: True)
    
    Returns:
        Dictionary containing refresh status, data age, and any errors.
        Will only refresh if force_refresh=True or if no data exists.
    """
    
    try:
        # Check API health first
        if not await api_client.health_check():
            return {
                "error": "USDA data API is not available. Please ensure the API server is running.",
                "status": "api_unavailable"
            }
        
        # Get current data summary to check freshness
        summary = await api_client.get_data_summary()
        latest_import = summary.get("latest_import", {})
        
        if not latest_import and not force_refresh:
            return {
                "status": "no_data_found",
                "message": "No existing data found. Use force_refresh=True to perform initial data collection.",
                "recommendation": "refresh_data(force_refresh=True)"
            }
        
        # Only auto-refresh if explicitly forced - never automatically refresh based on age
        data_age_days = 0
        freshness_status = "unknown"
        
        if latest_import.get("processed_at"):
            try:
                processed_at = datetime.fromisoformat(latest_import["processed_at"])
                data_age = datetime.now() - processed_at
                data_age_days = data_age.days
                
                # Determine freshness status using same logic as _get_data_freshness_info
                if data_age_days < 8:
                    freshness_status = "very_fresh"
                elif data_age_days <= 14:
                    freshness_status = "fresh"
                elif data_age_days <= 21:
                    freshness_status = "acceptable"
                elif data_age_days <= 27:
                    freshness_status = "getting_stale"
                else:
                    freshness_status = "stale"
                    
            except ValueError:
                freshness_status = "unknown"
        
        # Only refresh if explicitly forced
        if not force_refresh:
            return {
                "status": "refresh_not_requested",
                "message": f"Data is {data_age_days} days old ({freshness_status}). Use force_refresh=True to refresh.",
                "data_age_days": data_age_days,
                "freshness_status": freshness_status,
                "last_update": latest_import.get("processed_at"),
                "total_records": latest_import.get("row_count", 0),
                "recommendation": "Set force_refresh=True to update data, or continue using current data."
            }
        
        # Perform data refresh
        refresh_result = await _perform_data_refresh(progress_updates)
        
        return refresh_result
        
    except Exception as e:
        return {
            "error": f"Error checking or refreshing data: {str(e)}",
            "status": "refresh_error"
        }

async def _perform_data_refresh(progress_updates: bool = True) -> dict:
    """
    Perform the complete data refresh pipeline
    
    Returns detailed results of the refresh process
    """
    progress_log = []
    
    def log_progress(message: str):
        if progress_updates:
            progress_log.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    try:
        log_progress("Starting data refresh process...")
        
        # Step 1: Run the scraper to download fresh data
        log_progress("Step 1/3: Downloading fresh data from USDA Rural Data Gateway...")
        
        # Get the project root directory
        project_root = Path(__file__).parent.parent
        scraper_script = project_root / "scraper" / "download_data.py"
        
        if not scraper_script.exists():
            return {
                "error": f"Scraper script not found at {scraper_script}",
                "status": "scraper_missing",
                "progress_log": progress_log
            }
        
        # Run the scraper
        scraper_process = await asyncio.create_subprocess_exec(
            sys.executable, str(scraper_script),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(project_root)
        )
        
        stdout, stderr = await scraper_process.communicate()
        
        if scraper_process.returncode != 0:
            error_msg = stderr.decode('utf-8') if stderr else "Unknown scraper error"
            log_progress(f"Scraper failed: {error_msg}")
            return {
                "error": f"Data download failed: {error_msg}",
                "status": "download_failed",
                "progress_log": progress_log
            }
        
        log_progress("✓ Fresh data downloaded successfully")
        
        # Step 2: Process the data through the API (which handles structured table population)
        log_progress("Step 2/3: Processing data and updating structured tables...")
        
        # The API automatically processes new CSV files, so we need to trigger this
        # We'll check if new data was processed by calling the summary endpoint
        await asyncio.sleep(2)  # Give the system a moment to detect new files
        
        # Get updated summary
        updated_summary = await api_client.get_data_summary()
        
        log_progress("✓ Data processed and structured tables updated")
        
        # Step 3: Rebuild aggregation tables
        log_progress("Step 3/3: Rebuilding aggregation tables for fast queries...")
        
        # We need to trigger aggregation rebuild through the API or directly
        # For now, let's call it directly
        rebuild_script = project_root / "rebuild_aggregations.py"
        
        if rebuild_script.exists():
            rebuild_process = await asyncio.create_subprocess_exec(
                sys.executable, str(rebuild_script),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(project_root)
            )
            
            rebuild_stdout, rebuild_stderr = await rebuild_process.communicate()
            
            if rebuild_process.returncode == 0:
                log_progress("✓ Aggregation tables rebuilt successfully")
            else:
                log_progress(f"⚠ Aggregation rebuild had issues: {rebuild_stderr.decode('utf-8') if rebuild_stderr else 'Unknown error'}")
        else:
            log_progress("⚠ Aggregation rebuild script not found, tables may not be current")
        
        # Get final summary with updated data
        final_summary = await api_client.get_data_summary()
        latest_import = final_summary.get("latest_import", {})
        
        log_progress("🎉 Data refresh completed successfully!")
        
        return {
            "status": "refresh_completed",
            "message": "Data refresh completed successfully",
            "progress_log": progress_log,
            "new_data_summary": {
                "total_imports": final_summary.get("total_imports", 0),
                "latest_import_records": latest_import.get("row_count", 0),
                "latest_import_time": latest_import.get("processed_at"),
                "data_columns": len(latest_import.get("columns", []))
            },
            "refresh_duration": len(progress_log),  # Rough indicator of time taken
            "pipeline_steps_completed": [
                "Fresh data download",
                "Structured table update", 
                "Aggregation table rebuild"
            ]
        }
        
    except Exception as e:
        log_progress(f"❌ Refresh failed: {str(e)}")
        return {
            "error": f"Data refresh pipeline failed: {str(e)}",
            "status": "pipeline_error",
            "progress_log": progress_log
        }

if __name__ == "__main__":
    print("Starting USDA Rural Data Gateway MCP Server...")
    print("This server provides LLM access to USDA Rural Investment data")
    
    # Run the MCP server
    mcp.run()