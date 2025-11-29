from neo4j import GraphDatabase
import sqlite3
import pandas as pd
import os
from typing import Dict, List, Any
from dotenv import load_dotenv
import re

load_dotenv()

class TableToKGConverter:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"), 
            auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
        )
    
    def convert(self, db_path: str, table_name: str):
        """Convert ANY SQLite table → Neo4j Knowledge Graph"""
        
        # 1. Dynamic schema ingestion
        from src.ingestion.sqlite_ingestor import SQLiteIngestor
        ingestor = SQLiteIngestor(db_path)
        schema = ingestor.get_table_schema(table_name)
        
        print(f"📥 Ingested {schema['row_count']} rows from {table_name}")
        print(f"📋 Columns: {', '.join([c['name'] for c in schema['columns']])}")
        
        # 2. Clear Neo4j
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:`{table_name}`) REQUIRE n.{schema['primary_key']} IS UNIQUE")
        
        # 3. Create main entities (dynamic)
        self._create_main_entities(schema, table_name)
        
        # 4. Create relationships (dynamic)
        self._create_relationships(schema, table_name, db_path)
        
        print("✅ Dynamic Knowledge Graph Created!")
        print(f"🔗 Entities: {table_name} | Relationships: Auto-detected")
    
    def _create_main_entities(self, schema: Dict, table_name: str):
        """Create main entity nodes from ANY table"""
        df = schema['data']
        
        with self.driver.session() as session:
            # Create nodes for each row
            session.run(f"""
                UNWIND $rows as row
                MERGE (n:`{table_name}` {{`{schema['primary_key']}`: row.`{schema['primary_key']}`}})
                SET n += row
            """, rows=df.to_dict('records'))
    
    def _create_relationships(self, schema: Dict, source_table: str, db_path: str):
        """Auto-create relationships based on column patterns"""
        df = schema['data']
        
        # Pattern-based relationship detection
        relationship_patterns = [
            ('_id$', 'BELONGS_TO'),      # user_id → User
            ('_id', 'BELONGS_TO'),       # manager_id → Manager
            ('id$', 'BELONGS_TO'),       # category_id → Category
        ]
        
        conn = sqlite3.connect(db_path)
        
        for col in df.columns:
            for pattern, rel_type in relationship_patterns:
                if re.search(pattern, col):
                    target_label = col.replace('_id', '').replace('id', '').title()
                    
                    # Create relationships
                    with self.driver.session() as session:
                        session.run(f"""
                            UNWIND $rows as row
                            MATCH (source:`{source_table}` {{`{schema['primary_key']}`: row.`{schema['primary_key']}`}})
                            WHERE row.`{col}` IS NOT NULL
                            MERGE (target:`{target_label}` {{id: row.`{col}`}})
                            MERGE (source)-[:{rel_type} {{column: '{col}'}}]->(target)
                        """, rows=df.to_dict('records'))
        
        conn.close()
        
        # Special case: categorical columns → Category nodes
        categorical_cols = [col for col in df.columns if df[col].dtype == 'object' and col not in [schema['primary_key']]]
        for col in categorical_cols[:3]:  # Limit to avoid explosion
            with self.driver.session() as session:
                session.run(f"""
                    UNWIND $rows as row
                    MATCH (source:`{source_table}` {{`{schema['primary_key']}`: row.`{schema['primary_key']}`}})
                    WHERE row.`{col}` IS NOT NULL
                    MERGE (cat:Category {{name: row.`{col}`}})
                    MERGE (source)-[:HAS_CATEGORY {{column: '{col}'}}]->(cat)
                """, rows=df.to_dict('records'))
    
    def close(self):
        self.driver.close()
