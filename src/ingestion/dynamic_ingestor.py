import sqlite3
import pandas as pd
from typing import Dict, List, Any
import os

class DynamicIngestor:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.tables = []
        self.schemas = {}
    
    def ingest_all_tables(self) -> Dict[str, Any]:
        """Dynamically ingest ALL tables from ANY SQLite DB"""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get all tables
        tables_df = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'", 
            conn
        )
        self.tables = tables_df['name'].tolist()
        
        schemas = {}
        for table_name in self.tables:
            print(f"📋 Analyzing table: {table_name}")
            
            # Get schema
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [{'name': r[1], 'type': r[2], 'pk': r[5], 'notnull': r[3]} for r in cursor.fetchall()]
            
            # Get row count and sample
            row_count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name}", conn)['count'][0]
            sample_df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", conn)
            
            # Auto-detect PK
            pk_col = next((c['name'] for c in columns if c['pk']), columns[0]['name'] if columns else None)
            
            schemas[table_name] = {
                'columns': columns,
                'primary_key': pk_col,
                'row_count': row_count,
                'sample_data': sample_df.to_dict('records')
            }
        
        conn.close()
        self.schemas = schemas
        
        print(f"✅ Ingested {len(self.tables)} tables")
        return {
            'tables': self.tables,
            'schemas': schemas,
            'total_rows': sum(s['row_count'] for s in schemas.values())
        }
