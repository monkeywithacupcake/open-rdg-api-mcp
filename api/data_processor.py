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
        
        # stash the table as raw
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rural_investments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT,
                processed_data TEXT
            )
        """)

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

    def _build_aggregation_tables(self, cursor):
        """
        Kill if exist and then 
        Rebuild all aggregation tables from current structured data
        """
        print("Making aggregation tables...")
        
        # Clear existing aggregations if exist
        cursor.execute("DROP TABLE IF EXISTS state_year_summary")
        cursor.execute("DROP TABLE IF EXISTS program_year_summary") 
        cursor.execute("DROP TABLE IF EXISTS state_program_year_summary")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS state_year_summary (
                state_name TEXT,
                fiscal_year INTEGER,
                total_investments INTEGER,
                total_dollars REAL,
                avg_investment REAL,
                min_investment REAL,
                max_investment REAL,
                unique_programs INTEGER,
                top_programs TEXT, -- JSON array
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (state_name, fiscal_year)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS program_year_summary (
                program_area TEXT,
                fiscal_year INTEGER,
                total_investments INTEGER,
                total_dollars REAL,
                avg_investment REAL,
                min_investment REAL,
                max_investment REAL,
                unique_states INTEGER,
                top_states TEXT, -- JSON array
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (program_area, fiscal_year)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS state_program_year_summary (
                state_name TEXT,
                program_area TEXT,
                fiscal_year INTEGER,
                total_investments INTEGER,
                total_dollars REAL,
                avg_investment REAL,
                min_investment REAL,
                max_investment REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (state_name, program_area, fiscal_year)
            )
        """)
        
        # state_year_summary
        cursor.execute("""
            INSERT INTO state_year_summary (
                state_name, fiscal_year, total_investments, total_dollars,
                avg_investment, min_investment, max_investment, unique_programs, top_programs
            )
            SELECT 
                state_name,
                fiscal_year,
                COUNT(*) as total_investments,
                SUM(investment_dollars_numeric) as total_dollars,
                ROUND(AVG(investment_dollars_numeric), 2) as avg_investment,
                MIN(investment_dollars_numeric) as min_investment,
                MAX(investment_dollars_numeric) as max_investment,
                COUNT(DISTINCT program_area) as unique_programs,
                '[]' as top_programs  -- TODO: Calculate top programs JSON
            FROM investments
            WHERE investment_dollars_numeric > 0
            GROUP BY state_name, fiscal_year
        """)
        
        # program_year_summary
        cursor.execute("""
            INSERT INTO program_year_summary (
                program_area, fiscal_year, total_investments, total_dollars,
                avg_investment, min_investment, max_investment, unique_states, top_states
            )
            SELECT 
                program_area,
                fiscal_year,
                COUNT(*) as total_investments,
                SUM(investment_dollars_numeric) as total_dollars,
                ROUND(AVG(investment_dollars_numeric), 2) as avg_investment,
                MIN(investment_dollars_numeric) as min_investment,
                MAX(investment_dollars_numeric) as max_investment,
                COUNT(DISTINCT state_name) as unique_states,
                '[]' as top_states  -- TODO: Calculate top states JSON
            FROM investments
            WHERE investment_dollars_numeric > 0 AND program_area IS NOT NULL
            GROUP BY program_area, fiscal_year
        """)
        
        # state_program_year_summary
        cursor.execute("""
            INSERT INTO state_program_year_summary (
                state_name, program_area, fiscal_year, total_investments, total_dollars,
                avg_investment, min_investment, max_investment
            )
            SELECT 
                state_name,
                program_area,
                fiscal_year,
                COUNT(*) as total_investments,
                SUM(investment_dollars_numeric) as total_dollars,
                ROUND(AVG(investment_dollars_numeric), 2) as avg_investment,
                MIN(investment_dollars_numeric) as min_investment,
                MAX(investment_dollars_numeric) as max_investment
            FROM investments
            WHERE investment_dollars_numeric > 0 AND program_area IS NOT NULL
            GROUP BY state_name, program_area, fiscal_year
        """)
        
        print("Aggregation tables built or rebuilt successfully")

    def get_state_summary(self, state: str = None, fiscal_year: int = None) -> Dict:
        """
        Get pre-computed state summary data
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if state and fiscal_year:
                # Specific state and year
                cursor.execute("""
                    SELECT state_name, fiscal_year, total_investments, total_dollars,
                           avg_investment, min_investment, max_investment, unique_programs
                    FROM state_year_summary 
                    WHERE state_name = ? AND fiscal_year = ?
                """, (state, fiscal_year))
                result = cursor.fetchone()
                
                if result:
                    return {
                        "state_name": result[0],
                        "fiscal_year": result[1], 
                        "total_investments": result[2],
                        "total_dollars": result[3],
                        "avg_investment": result[4],
                        "min_investment": result[5],
                        "max_investment": result[6],
                        "unique_programs": result[7]
                    }
                else:
                    return {"error": f"No data found for {state} in {fiscal_year}"}
                    
            elif state:
                # All years for a state
                cursor.execute("""
                    SELECT fiscal_year, total_investments, total_dollars, avg_investment
                    FROM state_year_summary 
                    WHERE state_name = ?
                    ORDER BY fiscal_year
                """, (state,))
                results = cursor.fetchall()
                
                return {
                    "state_name": state,
                    "years": [
                        {
                            "fiscal_year": row[0],
                            "total_investments": row[1], 
                            "total_dollars": row[2],
                            "avg_investment": row[3]
                        } for row in results
                    ]
                }
            else:
                # Top states summary
                cursor.execute("""
                    SELECT state_name, SUM(total_investments) as total_inv, 
                           SUM(total_dollars) as total_dollars,
                           AVG(avg_investment) as avg_inv
                    FROM state_year_summary 
                    GROUP BY state_name
                    ORDER BY total_dollars DESC
                    LIMIT 10
                """)
                results = cursor.fetchall()
                
                return {
                    "top_states": [
                        {
                            "state_name": row[0],
                            "total_investments": row[1],
                            "total_dollars": row[2], 
                            "avg_investment": row[3]
                        } for row in results
                    ]
                }
                
        except Exception as e:
            raise Exception(f"Error getting state summary: {e}")
        finally:
            conn.close()
    
    def get_program_summary(self, program: str = None, fiscal_year: int = None) -> Dict:
        """
        Get pre-computed program summary data
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if program and fiscal_year:
                # Specific program and year
                cursor.execute("""
                    SELECT program_area, fiscal_year, total_investments, total_dollars,
                           avg_investment, min_investment, max_investment, unique_states
                    FROM program_year_summary 
                    WHERE program_area = ? AND fiscal_year = ?
                """, (program, fiscal_year))
                result = cursor.fetchone()
                
                if result:
                    return {
                        "program_area": result[0],
                        "fiscal_year": result[1], 
                        "total_investments": result[2],
                        "total_dollars": result[3],
                        "avg_investment": result[4],
                        "min_investment": result[5],
                        "max_investment": result[6],
                        "unique_states": result[7]
                    }
                else:
                    return {"error": f"No data found for {program} in {fiscal_year}"}
                    
            else:
                # All programs summary
                cursor.execute("""
                    SELECT program_area, SUM(total_investments) as total_inv,
                           SUM(total_dollars) as total_dollars,
                           AVG(avg_investment) as avg_inv
                    FROM program_year_summary 
                    GROUP BY program_area
                    ORDER BY total_dollars DESC
                """)
                results = cursor.fetchall()
                
                return {
                    "programs": [
                        {
                            "program_area": row[0],
                            "total_investments": row[1],
                            "total_dollars": row[2],
                            "avg_investment": row[3]
                        } for row in results
                    ]
                }
                
        except Exception as e:
            raise Exception(f"Error getting program summary: {e}")
        finally:
            conn.close()

    def store_dataframe(self, df: pd.DataFrame, source_file: Path):
        """
        Store processed DataFrame in structured +agg tables
        """
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.cursor()

            # Store raw data as JSON (backup/compatibility)
            raw_data = df.to_json(orient='records')
            summary = {
                'source_file': str(source_file),
                'row_count': len(df),
                'columns': list(df.columns),
                'processed_at': datetime.now().isoformat()
            }
            
            cursor.execute("""
                INSERT INTO rural_investments (raw_data, processed_data)
                VALUES (?, ?)
            """, (raw_data, json.dumps(summary)))

            # Clear existing data for fresh import
            # otherwise you will have duplicates
            cursor.execute("DELETE FROM investments")
            
            # csv -> db + agg tables
            self._insert_structured_data(cursor, df)
            self._build_aggregation_tables(cursor)
            
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
    
    
    def get_data_summary(self) -> Dict:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # this is looking at the number of tables we 
        # have imported. decide if want to keep later
        # useful for testing
        cursor.execute("SELECT COUNT(*) FROM rural_investments")
        total_imports = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT processed_data FROM rural_investments 
            ORDER BY imported_at DESC LIMIT 1
        """)
        latest = cursor.fetchone()
        
        conn.close()
        
        summary = {
            'total_imports': total_imports,
            'latest_import': json.loads(latest[0]) if latest else None
        }
        
        return summary

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
    
def find_newest_csv(directory_path):
    """We only want newest csv"""
    try:
        data_dir = Path(directory_path) # only the detail works right now
        csv_files = data_dir.glob("*_detail.csv")
        
        if not csv_files:
            print("There is no csv, you may need to run `uv run fetch/download_data.py`")
            return None # No files found

        # Use the `key=os.path.getmtime` argument to find the most recently modified file
        newest_file = max(csv_files, key=os.path.getmtime)
        
        return newest_file
    
    except FileNotFoundError:
        print(f"Error: The directory '{directory_path}' does not exist.")
        return None

if __name__ == "__main__":
    processor = USDADataProcessor()
    # now run the process on our csv file
    data_dir = Path("./data")
    csv_file = find_newest_csv(data_dir)
    processor.process_csv(csv_file)
    # Print summary
    summary = processor.get_data_summary()
    print("Data Summary:", json.dumps(summary, indent=2))