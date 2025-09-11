Stashed notes for the api

Setup server.py to allow for more complicated queries with the /investments/query and /summary/query

I keep forgetting what these are for, so this is a note


1. Complex Filter Objects
# nested filters
  {
    "filters": {
      "state": "Texas",
      "investment_dollars_numeric": {"min": 100000, "max": 1000000},
      "program_area": ["Rural Housing", "Rural Business"],
      "borrower_name": {"contains": "LLC"}
    },
    "limit": 50
  }

  2. Multiple Value Filtering

  # Filter by multiple states or programs
  {
    "filters": {
      "state": ["Texas", "California", "New York"],
      "fiscal_year": [2022, 2023, 2024]
    }
  }

  3. Range Queries

  # Investment amount ranges, date ranges
  {
    "filters": {
      "investment_dollars_numeric": {"gte": 500000},
      "fiscal_year": {"between": [2020, 2023]},
      "zip_code": {"startswith": "75"}
    }
  }

  4. Text Search Operations

  # Complex text matching
  {
    "filters": {
      "borrower_name": {"regex": ".*Hospital.*|.*Medical.*"},
      "project_name": {"contains": "broadband"},
      "city": {"in": ["Dallas", "Houston", "Austin"]}
    }
  }