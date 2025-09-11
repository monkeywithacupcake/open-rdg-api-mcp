#!/usr/bin/env python3
"""
Data processing module for USDA Rural Data Gateway data
Users will download a csv, but we want to make it easy to consume
Handles CSV parsing, cleaning, and database storage
"""

import pandas as pd
import sqlite3
from pathlib import Path
import os
from typing import Optional, Dict
from datetime import datetime
import json

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
        # this is based off of _detail
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
                data_source_file TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # SUMMARY table (based off of _hist)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fiscal_year INTEGER NOT NULL,
                state_name TEXT NOT NULL,
                program_area TEXT NOT NULL,
                investment_dollars_numeric REAL,
                investment_dollars_original TEXT,
                number_of_investments INTEGER,               
                data_source_file TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(fiscal_year, state_name, program_area)
            )
        """)
        
        # INVESTMENTS indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inv_state_name ON INVESTMENTS(state_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inv_program_area ON INVESTMENTS(program_area)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inv_investment_dollars ON INVESTMENTS(investment_dollars_numeric)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inv_state_year ON INVESTMENTS(state_name, fiscal_year)")

        # SUMMARY indexes (i feel like this might be over kill but hwy not)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sum_state_name ON SUMMARY(state_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sum_program_area ON SUMMARY(program_area)")
        conn.commit()
        conn.close()
    
    # TODO; separate latest file from type
    def _find_latest_file(self, data_dir: Path, file_type: str) -> Optional[Path]:
        """Find the most recent file of the specified type based on timestamp in filename"""
        files = []
        for csv_file in data_dir.glob("usda_rural_*.csv"):
            filename = csv_file.name.lower()
            if file_type in filename:
                try:
                    timestamp_part = filename.split('_')[-1].replace('.csv', '')
                    timestamp = int(timestamp_part)
                    files.append((timestamp, csv_file))
                except (ValueError, IndexError):
                    print(f"Could not extract timestamp from {file_type} file: {filename}")
        
        if files:
            files.sort(key=lambda x: x[0], reverse=True)
            return files[0][1]
        return None

    def process_all_csvs(self) -> Dict[str, int]:
        """
        Process the most recent CSV files in the data directory
        Only processes ONE detail file and ONE hist file (the most recent of each)
        """
        data_dir = self.db_path.parent
        results = {'detail_files': 0, 'summary_files': 0, 'unknown_files': 0, 'errors': 0}

        # Find the most recent files of each type
        latest_detail_file = self._find_latest_file(data_dir, 'detail')
        latest_hist_file = self._find_latest_file(data_dir, 'hist')
        
        if latest_detail_file:
            print(f"Most recent detail file: {latest_detail_file.name}")
        if latest_hist_file:
            print(f"Most recent hist file: {latest_hist_file.name}")
        
        # Process files using a mapping approach
        file_processors = [
            (latest_detail_file, self.process_detail_csv, 'detail_files', 'detail'),
            (latest_hist_file, self.process_summary_csv, 'summary_files', 'hist')
        ]
        
        for file_path, processor_func, result_key, file_type in file_processors:
            if file_path:
                try:
                    print(f"Processing {file_type} file: {file_path}")
                    success = processor_func(file_path)
                    if success:
                        results[result_key] += 1
                    else:
                        results['errors'] += 1
                except Exception as e:
                    print(f"Error processing {file_type} file {file_path}: {e}")
                    results['errors'] += 1
        
        if not latest_detail_file and not latest_hist_file:
            print("No valid detail or hist files found to process")
        
        return results
    
    def _read_csv(self, csv_path:Path) -> pd.DataFrame:
        try:
            print(f"Processing detail CSV: {csv_path}")
            
            # Try different encodings
            encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding, sep='\t', low_memory=False)
                    print(f"Successfully loaded detail CSV with encoding: {encoding}")
                    break
                except (UnicodeDecodeError, Exception):
                    continue
            
            if df is None:
                print(f"Could not read detail CSV {csv_path}")
                return False
            
            print(f"Loaded {len(df)} detail records")
                        
        except Exception as e:
            print(f"Error processing detail CSV {csv_path}: {e}")
            return False
        return df

    def process_detail_csv(self, csv_path: Path) -> bool:
        """Process detailed transaction CSV and store in INVESTMENTS table"""
        df = self._read_csv(csv_path)
        df = self._clean_detail_data(df)
        # Store in INVESTMENTS table
        return self._store_detail_data(df, csv_path)

    
    def process_summary_csv(self, csv_path: Path) -> bool:
        """Process historical summary CSV and store in SUMMARY table"""
        df = self._read_csv(csv_path)
        df = self._clean_summary_data(df)
        # Store in SUMMARY table
        return self._store_summary_data(df, csv_path)
    
    def _clean_detail_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize detailed transaction data"""
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)
        
        # Convert investment_dollars to numeric
        if 'investment_dollars' in df.columns:
            df['investment_dollars_original'] = df['investment_dollars'].astype(str)
            df['investment_dollars_numeric'] = df['investment_dollars'].apply(self._convert_dollars_to_numeric)
        
        # Ensure fiscal_year is integer
        if 'fiscal_year' in df.columns:
            df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce').fillna(0).astype(int)
        
        # Ensure number_of_investments is integer
        if 'number_of_investments' in df.columns:
            df['number_of_investments'] = pd.to_numeric(df['number_of_investments'], errors='coerce').fillna(1).astype(int)
        
        return df
    
    def _clean_summary_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize summary data"""
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)
        
        # Convert investment_dollars to numeric
        if 'investment_dollars' in df.columns:
            df['investment_dollars_original'] = df['investment_dollars'].astype(str)
            df['investment_dollars_numeric'] = df['investment_dollars'].apply(self._convert_dollars_to_numeric)
                
        # Ensure fiscal_year is integer
        if 'fiscal_year' in df.columns:
            df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce').fillna(0).astype(int)
        
        # Ensure number_of_investments is integer
        if 'number_of_investments' in df.columns:
            df['number_of_investments'] = pd.to_numeric(df['number_of_investments'], errors='coerce').fillna(1).astype(int)
        
        return df
    
    def _convert_dollars_to_numeric(self, value) -> float:
        """Convert investment dollar strings to numeric values"""
        if pd.isna(value) or value in ['', 'Not Available', 'Withheld', 'NOT AVAILABLE']:
            return 0.0
        
        try:
            if isinstance(value, str):
                # Remove commas, dollar signs, spaces
                cleaned = value.replace(',', '').replace('$', '').replace(' ', '').replace('Â', '')
                return float(cleaned)
            else:
                return float(value)
        except (ValueError, TypeError):
            print(f"Warning: Could not convert value to numeric: {value}")
            return 0.0
    
    def _store_detail_data(self, df: pd.DataFrame, source_file: Path) -> bool:
        """Store detailed data in INVESTMENTS table"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.cursor()
            
            # Clear existing data 
            cursor.execute("DELETE FROM INVESTMENTS")
            
            # Column mapping
            detail_columns = [
                'fiscal_year', 'state_name', 'county', 'county_fips', 'congressional_district',
                'program_area', 'program', 'zip_code', 'persistent_poverty_community_status',
                'borrower_name', 'project_name', 'investment_type', 'city', 'lender_name',
                'funding_code', 'naics_industry_sector', 'naics_national_industry_code',
                'naics_national_industry', 'portfolio_type', 'project_announced_description',
                'investment_dollars_numeric', 'investment_dollars_original', 'number_of_investments'
            ]
            
            records = []
            for _, row in df.iterrows():
                record = []
                for col in detail_columns:
                    if col in df.columns:
                        value = row[col]
                        record.append(None if pd.isna(value) else value)
                    else:
                        record.append(None)
                record.append(str(source_file))  # data_source_file
                records.append(tuple(record))
            
            # Insert records
            placeholders = ', '.join(['?' for _ in detail_columns] + ['?'])  # +1 for data_source_file
            cursor.executemany(f"""
                INSERT INTO INVESTMENTS ({', '.join(detail_columns)}, data_source_file)
                VALUES ({placeholders})
            """, records)
            
            conn.commit()
            print(f"Stored {len(records)} detail records from {source_file}")
            return True
            
        except Exception as e:
            print(f"Error storing detail data: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def _store_summary_data(self, df: pd.DataFrame, source_file: Path) -> bool:
        """Store summary data in SUMMARY table"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.cursor()
            
            # Clear existing data from this source file
            cursor.execute("DELETE FROM SUMMARY")
            
            # Column mapping for summary data
            summary_columns = [
                'fiscal_year', 'state_name', 'program_area', 'investment_dollars_numeric',
                'investment_dollars_original', 'number_of_investments'
            ]
            
            records = []
            for _, row in df.iterrows():
                record = []
                for col in summary_columns:
                    if col in df.columns:
                        value = row[col]
                        record.append(None if pd.isna(value) else value)
                    else:
                        record.append(None)
                record.append(str(source_file))  # data_source_file
                records.append(tuple(record))
            
            # Insert records with REPLACE to handle duplicates
            placeholders = ', '.join(['?' for _ in summary_columns] + ['?'])  # +1 for data_source_file
            cursor.executemany(f"""
                INSERT OR REPLACE INTO SUMMARY ({', '.join(summary_columns)}, data_source_file)
                VALUES ({placeholders})
            """, records)
            
            conn.commit()
            print(f"Stored {len(records)} summary records from {source_file}")
            return True
            
        except Exception as e:
            print(f"Error storing summary data: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

      
    def get_data_summary(self) -> Dict:
        """Get summary of all processed data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # INVESTMENTS table summary
            cursor.execute("SELECT SUM(number_of_investments), MIN(fiscal_year), MAX(fiscal_year) FROM INVESTMENTS")
            inv_count, inv_min_year, inv_max_year = cursor.fetchone()
            
            # SUMMARY table summary
            cursor.execute("SELECT SUM(number_of_investments), MIN(fiscal_year), MAX(fiscal_year) FROM SUMMARY")
            sum_count, sum_min_year, sum_max_year = cursor.fetchone()
            
            # Total investment amounts
            cursor.execute("SELECT SUM(investment_dollars_numeric) FROM INVESTMENTS")
            inv_total = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT SUM(investment_dollars_numeric) FROM SUMMARY")
            sum_total = cursor.fetchone()[0] or 0
            
            return {
                'investments_table': {
                    'record_count': inv_count,
                    'fiscal_year_range': f"{inv_min_year}-{inv_max_year}" if inv_min_year else "No data",
                    'total_dollars': inv_total
                },
                'summary_table': {
                    'record_count': sum_count,
                    'fiscal_year_range': f"{sum_min_year}-{sum_max_year}" if sum_min_year else "No data",
                    'total_dollars': sum_total
                },
                'last_updated': datetime.now().isoformat()
            }
        finally:
            conn.close()
    
    def query_investments(self, filters: Dict = None, limit: int = 100, offset: int = 0) -> Dict:
        """Query detailed investment data with filters"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
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
            cursor.execute(f"SELECT SUM(number_of_investments) FROM INVESTMENTS{where_clause}", params)
            total = cursor.fetchone()[0]
            
            # Get paginated data
            cursor.execute(f"""
                SELECT fiscal_year, state_name, county, program_area, program,
                       investment_dollars_numeric, number_of_investments, borrower_name,
                       city, lender_name, project_name, investment_type
                FROM INVESTMENTS{where_clause}
                ORDER BY investment_dollars_numeric DESC
                LIMIT ? OFFSET ?
            """, params + [limit, offset])
            
            columns = [desc[0] for desc in cursor.description]
            records = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return {
                "data": records,
                "total": total,
                "limit": limit,
                "offset": offset,
                "returned": len(records)
            }
            
        finally:
            conn.close()
    
    def query_summary(self, filters: Dict = None, limit: int = 100, offset: int = 0) -> Dict:
        """Query summary data with filters"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
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
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Get total count
            cursor.execute(f"SELECT SUM(number_of_investments) FROM SUMMARY{where_clause}", params)
            total = cursor.fetchone()[0]
            
            # Get paginated data
            cursor.execute(f"""
                SELECT fiscal_year, state_name, program_area, investment_dollars_numeric,
                       number_of_investments
                FROM SUMMARY{where_clause}
                ORDER BY investment_dollars_numeric DESC
                LIMIT ? OFFSET ?
            """, params + [limit, offset])
            
            columns = [desc[0] for desc in cursor.description]
            records = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return {
                "data": records,
                "total": total,
                "limit": limit,
                "offset": offset,
                "returned": len(records)
            }
            
        finally:
            conn.close()
    
    def get_state_aggregations(self, state: str = None, fiscal_year: int = None) -> Dict:
        """Get state aggregations by querying SUMMARY table directly"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if state and fiscal_year:
                # Query SUMMARY table for specific state and year
                cursor.execute("""
                    SELECT 
                        state_name,
                        fiscal_year,
                        SUM(investment_dollars_numeric) as total_investment_dollars,
                        SUM(number_of_investments) as total_number_of_investments,
                        COUNT(DISTINCT program_area) as unique_programs,
                        ROUND(AVG(CASE WHEN investment_dollars_numeric > 0 THEN investment_dollars_numeric END), 2) as avg_investment_size
                    FROM SUMMARY 
                    WHERE state_name = ? AND fiscal_year = ?
                    GROUP BY state_name, fiscal_year
                """, (state, fiscal_year))
                result = cursor.fetchone()
                
                if result:
                    return {
                        "state_name": result[0],
                        "fiscal_year": result[1],
                        "total_investment_dollars": result[2],
                        "total_number_of_investments": result[3],
                        "unique_programs": result[4],
                        "avg_investment_size": result[5]
                    }
                else:
                    return {"error": f"No data found for {state} in {fiscal_year}"}
            
            elif state:
                # All years for a specific state
                cursor.execute("""
                    SELECT 
                        fiscal_year,
                        SUM(investment_dollars_numeric) as total_investment_dollars,
                        SUM(number_of_investments) as total_number_of_investments,
                        ROUND(AVG(CASE WHEN investment_dollars_numeric > 0 THEN investment_dollars_numeric END), 2) as avg_investment_size
                    FROM SUMMARY 
                    WHERE state_name = ?
                    GROUP BY fiscal_year
                    ORDER BY fiscal_year
                """, (state,))
                results = cursor.fetchall()
                
                return {
                    "state_name": state,
                    "years": [
                        {
                            "fiscal_year": row[0],
                            "total_investment_dollars": row[1],
                            "total_number_of_investments": row[2],
                            "avg_investment_size": row[3]
                        } for row in results
                    ]
                }
            
            else:
                # Top states summary
                cursor.execute("""
                    SELECT 
                        state_name, 
                        SUM(investment_dollars_numeric) as total_dollars,
                        SUM(number_of_investments) as total_investments
                    FROM SUMMARY 
                    GROUP BY state_name
                    ORDER BY total_dollars DESC
                    LIMIT 10
                """)
                results = cursor.fetchall()
                
                return {
                    "top_states": [
                        {
                            "state_name": row[0],
                            "total_investment_dollars": row[1],
                            "total_number_of_investments": row[2]
                        } for row in results
                    ]
                }
        
        finally:
            conn.close()
    
    def get_program_aggregations(self, program: str = None, fiscal_year: int = None) -> Dict:
        """Get program aggregations by querying SUMMARY table directly"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if program and fiscal_year:
                # Query SUMMARY table for specific program and year
                cursor.execute("""
                    SELECT 
                        program_area,
                        fiscal_year,
                        SUM(investment_dollars_numeric) as total_investment_dollars,
                        SUM(number_of_investments) as total_number_of_investments,
                        COUNT(DISTINCT state_name) as unique_states,
                        ROUND(AVG(CASE WHEN investment_dollars_numeric > 0 THEN investment_dollars_numeric END), 2) as avg_investment_size
                    FROM SUMMARY 
                    WHERE program_area = ? AND fiscal_year = ?
                    GROUP BY program_area, fiscal_year
                """, (program, fiscal_year))
                result = cursor.fetchone()
                
                if result:
                    return {
                        "program_area": result[0],
                        "fiscal_year": result[1],
                        "total_investment_dollars": result[2],
                        "total_number_of_investments": result[3],
                        "unique_states": result[4],
                        "avg_investment_size": result[5]
                    }
                else:
                    return {"error": f"No data found for {program} in {fiscal_year}"}
            
            else:
                # All programs summary
                cursor.execute("""
                    SELECT 
                        program_area, 
                        SUM(investment_dollars_numeric) as total_dollars,
                        SUM(number_of_investments) as total_investments
                    FROM SUMMARY 
                    GROUP BY program_area
                    ORDER BY total_dollars DESC
                """)
                results = cursor.fetchall()
                
                return {
                    "programs": [
                        {
                            "program_area": row[0],
                            "total_investment_dollars": row[1],
                            "total_number_of_investments": row[2]
                        } for row in results
                    ]
                }
        
        finally:
            conn.close()

if __name__ == "__main__":
    processor = USDADataProcessor()
    # now run to process any csvs we have
    results = processor.process_all_csvs()
    print(f"Processing complete: {results}")
    
    # Print summary
    summary = processor.get_data_summary()
    print("Data Summary:", json.dumps(summary, indent=2))