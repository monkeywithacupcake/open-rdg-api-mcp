#!/usr/bin/env python3
"""
Data processing module for USDA Rural Data Gateway data
Users will download a csv, but we want to make it easy to consume
Handles CSV parsing, cleaning, and database storage
"""

import pandas as pd
import sqlite3
from pathlib import Path
from typing import Optional, Dict, List
import json
from datetime import datetime

class USDADataProcessor:
    def __init__(self, db_path="./data/usda_data.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with rural investments tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Structured table for indexed queries and aggregations
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS investments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_year INTEGER NOT NULL,
                state_name TEXT NOT NULL,
                county TEXT,
                county_fips TEXT,
                congressional_district TEXT,
                program_area TEXT,
                program TEXT,
                investment_type TEXT,
                investment_dollars_numeric REAL,
                investment_dollars_original TEXT,
                number_of_investments INTEGER,
                borrower_name TEXT,
                city TEXT,
                lender_name TEXT,
                project_name TEXT,
                funding_code TEXT,
                naics_industry_sector TEXT,
                portfolio_type TEXT,
                persistent_poverty_community_status TEXT,
                zip_code TEXT,
                naics_national_industry_code TEXT,
                naics_national_industry TEXT,
                project_announced_description TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Performance indexes on key query fields
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fiscal_year ON investments(fiscal_year)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_state_name ON investments(state_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_program_area ON investments(program_area)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_investment_dollars ON investments(investment_dollars_numeric)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_county_fips ON investments(county_fips)")
        
        # Composite indexes for common query patterns
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_state_year ON investments(state_name, fiscal_year)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_program_year ON investments(program_area, fiscal_year)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_state_program_year ON investments(state_name, program_area, fiscal_year)")
        
        conn.commit()
        conn.close()
    
    def process_csv(self, csv_path: Path) -> Optional[pd.DataFrame]:
        """
        Process a CSV file from USDA downloads
        Returns cleaned DataFrame
        """
        try:
            print(f"Processing CSV: {csv_path}")
            
            # Try different encodings for the CSV file
            encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    print(f"Trying encoding: {encoding}")
                    df = pd.read_csv(csv_path, encoding=encoding, sep='\t')  # Tab-separated based on head output
                    print(f"Successfully loaded with encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"Error with encoding {encoding}: {e}")
                    continue
            
            if df is None:
                print("Could not read CSV with any encoding")
                return None
            
            print(f"Loaded {len(df)} rows with columns: {list(df.columns)}")
            
            # Basic cleaning
            df = self.clean_data(df)
            
            # Store in database
            self.store_dataframe(df, csv_path)
            
            return df
            
        except Exception as e:
            print(f"Error processing CSV {csv_path}: {e}")
            return None
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the data
        """
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Standardize column names (lowercase, underscores)
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)
        
        # Convert investment_dollars from string to numeric
        if 'investment_dollars' in df.columns:
            # Store original values
            df['investment_dollars_original'] = df['investment_dollars'].astype(str)
            
            # Convert to numeric (remove commas, handle special cases)
            df['investment_dollars_numeric'] = df['investment_dollars'].apply(self._convert_dollars_to_numeric)
        
        # Convert date columns if present
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
        
        return df
    
    def _convert_dollars_to_numeric(self, value) -> float:
        """
        Convert investment dollar strings to numeric values
        Handles: "265,000", "1,234,567", etc.
        """
        if pd.isna(value) or value in ['', 'Not Available', 'Withheld']:
            return 0.0
        
        try:
            # Remove commas and convert to float
            if isinstance(value, str):
                # Remove commas, dollar signs, spaces
                cleaned = value.replace(',', '').replace('$', '').replace(' ', '')
                return float(cleaned)
            else:
                return float(value)
        except (ValueError, TypeError):
            print(f"Warning: Could not convert investment value to numeric: {value}")
            return 0.0
    
    def store_dataframe(self, df: pd.DataFrame, source_file: Path):
        """
        Store processed DataFrame in both JSON (backup) and structured tables
        """
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.cursor()
            
            # csv -> db 
            self._insert_structured_data(cursor, df)
            
            conn.commit()
            print(f"Stored {len(df)} records in structured database with indexes and aggregations")
            
        except Exception as e:
            print(f"Error storing data: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _insert_structured_data(self, cursor, df: pd.DataFrame):
        """
        Insert DataFrame records into structured investments table
        """
        # Column mapping from DataFrame to database
        column_mapping = {
            'fiscal_year': 'fiscal_year',
            'state_name': 'state_name', 
            'county': 'county',
            'county_fips': 'county_fips',
            'congressional_district': 'congressional_district',
            'program_area': 'program_area',
            'program': 'program',
            'investment_type': 'investment_type',
            'investment_dollars_numeric': 'investment_dollars_numeric',
            'investment_dollars_original': 'investment_dollars_original',
            'number_of_investments': 'number_of_investments',
            'borrower_name': 'borrower_name',
            'city': 'city',
            'lender_name': 'lender_name',
            'project_name': 'project_name',
            'funding_code': 'funding_code',
            'naics_industry_sector': 'naics_industry_sector',
            'portfolio_type': 'portfolio_type',
            'persistent_poverty_community_status': 'persistent_poverty_community_status',
            'zip_code': 'zip_code',
            'naics_national_industry_code': 'naics_national_industry_code',
            'naics_national_industry': 'naics_national_industry',
            'project_announced_description': 'project_announced_description'
        }
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = {}
            for df_col, db_col in column_mapping.items():
                if df_col in df.columns:
                    value = row[df_col]
                    # Handle NaN values
                    if pd.isna(value):
                        record[db_col] = None
                    else:
                        record[db_col] = value
                else:
                    record[db_col] = None
            records.append(record)
        
        # Insert records
        insert_sql = f"""
            INSERT INTO investments ({', '.join(column_mapping.values())})
            VALUES ({', '.join(['?' for _ in column_mapping])})
        """
        
        cursor.executemany(insert_sql, [
            tuple(record[col] for col in column_mapping.values()) 
            for record in records
        ])
        
        print(f"Inserted {len(records)} records into structured investments table")
    
    


    def query_structured_data(self, filters: Dict = None, limit: int = 100, offset: int = 0) -> Dict:
        """
        Query structured investment data with fast indexed queries
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Build WHERE clause from filters
            where_conditions = []
            params = []
            
            if filters:
                if filters.get('state'):
                    where_conditions.append("state_name = ?")
                    params.append(filters['state'])
                
                if filters.get('program'):
                    where_conditions.append("program_area LIKE ?")
                    params.append(f"%{filters['program']}%")
                
                if filters.get('fiscal_year'):
                    where_conditions.append("fiscal_year = ?")
                    params.append(filters['fiscal_year'])
                
                if filters.get('borrower_name'):
                    where_conditions.append("borrower_name LIKE ?")
                    params.append(f"%{filters['borrower_name']}%")
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Get total count
            count_sql = f"SELECT COUNT(*) FROM investments{where_clause}"
            cursor.execute(count_sql, params)
            total = cursor.fetchone()[0]
            
            # Get paginated data
            data_sql = f"""
                SELECT fiscal_year, state_name, county, program_area, program,
                       investment_dollars_numeric, number_of_investments, borrower_name,
                       city, lender_name, project_name, investment_type
                FROM investments{where_clause}
                ORDER BY investment_dollars_numeric DESC
                LIMIT ? OFFSET ?
            """
            cursor.execute(data_sql, params + [limit, offset])
            
            columns = [desc[0] for desc in cursor.description]
            records = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                records.append(record)
            
            return {
                "data": records,
                "total": total,
                "limit": limit,
                "offset": offset,
                "returned": len(records)
            }
            
        except Exception as e:
            raise Exception(f"Error querying structured data: {e}")
        finally:
            conn.close()
    


if __name__ == "__main__":
    processor = USDADataProcessor()
    
    # Process any CSV files in the data directory - should be only 1
    data_dir = Path("./data")
    for csv_file in data_dir.glob("*.csv"):
        processor.process_csv(csv_file)
    