import sqlite3
import pandas as pd
from typing import Dict, List, Any

class SQLiteIngestor:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_tables(self) -> List[str]:
        """Get all tables in database"""
        conn = sqlite3.connect(self.db_path)
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
        conn.close()
        return tables
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Dynamic schema detection for ANY table"""
        conn = sqlite3.connect(self.db_path)
        
        # Get all columns
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [dict(name=r[1], type=r[2], pk=r[5]) for r in cursor.fetchall()]
        
        # Get sample data
        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 100", conn)
        
        # Auto-detect primary key
        pk_col = next((c['name'] for c in columns if c['pk']), df.columns[0] if len(df.columns) > 0 else None)
        
        # Auto-detect relationships (foreign keys via common patterns)
        relationships = self._detect_relationships(table_name, df, conn)
        
        conn.close()
        
        return {
            'table_name': table_name,
            'columns': columns,
            'primary_key': pk_col,
            'data': df,
            'row_count': len(df),
            'relationships': relationships,
            'sample_data': df.head(5).to_dict('records')
        }
    
    def _detect_relationships(self, table_name: str, df: pd.DataFrame, conn: sqlite3.Connection) -> List[Dict]:
        """Auto-detect foreign key relationships"""
        relationships = []
        for col in df.columns:
            if col.lower() in ['id', '_id'] or col.endswith('_id'):
                # Check if this column references another table's PK
                unique_vals = df[col].dropna().unique()
                if len(unique_vals) < len(df) * 0.5:  # Likely a FK
                    # Find target table
                    target_table = self._find_target_table(unique_vals, conn)
                    if target_table:
                        relationships.append({
                            'source_table': table_name,
                            'source_column': col,
                            'target_table': target_table,
                            'relationship_type': 'BELONGS_TO'
                        })
        return relationships
    
    def _find_target_table(self, values: List, conn: sqlite3.Connection) -> str:
        """Find table that contains these values as primary key"""
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
        for table in tables:
            if table == 'sqlite_master':
                continue
            try:
                pk_count = pd.read_sql_query(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
                if pk_count == len(values):
                    return table
            except:
                continue
        return None
