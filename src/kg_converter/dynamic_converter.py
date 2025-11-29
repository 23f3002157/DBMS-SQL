from neo4j import GraphDatabase
import pandas as pd
import sqlite3
import os
from typing import Dict, List, Tuple
from dotenv import load_dotenv
import re
from ..ingestion.dynamic_ingestor import DynamicIngestor

load_dotenv()

class DynamicKGConverter:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"), 
            auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
        )
    
    def convert_all_tables(self, db_path: str):
        """FULL DENORMALIZATION: ALL tables → CONNECTED KG"""
        print(f"🔄 FULL DENORMALIZATION: {db_path} → Neo4j KG...")
        
        # 1. Ingest ALL tables
        ingestor = DynamicIngestor(db_path)
        schema_info = ingestor.ingest_all_tables()
        
        # 2. Clean Neo4j
        self._safe_cleanup()
        
        # 3. Create ALL nodes FIRST (base version)
        table_nodes = {}
        for table_name, schema in schema_info['schemas'].items():
            safe_label = self._safe_label(table_name)
            pk = schema['primary_key']
            print(f"  📦 Creating {table_name} nodes...")
            node_count = self._create_base_nodes(db_path, table_name, safe_label, pk)
            table_nodes[table_name] = {'label': safe_label, 'pk': pk, 'count': node_count}
        
        # 4. 🔥 CREATE ALL FK RELATIONSHIPS (MAIN FEATURE)
        relationships_created = self._create_all_fk_relationships(db_path, table_nodes, schema_info['schemas'])
        
        # 5. Create categorical relationships
        self._create_categorical_relationships(db_path, table_nodes, schema_info['schemas'])
        
        print(f"✅ FULL KG CREATED!")
        print(f"   📊 {sum(t['count'] for t in table_nodes.values())} total nodes")
        print(f"   🔗 {relationships_created} FK relationships")
        return schema_info
    
    def _create_base_nodes(self, db_path: str, table_name: str, safe_label: str, pk: str) -> int:
        """Create base nodes (without denormalization first)"""
        safe_pk = self._safe_property(pk)
        
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        # Create clean row dicts
        rows = []
        for _, row in df.iterrows():
            row_dict = {}
            for col, val in row.items():
                if pd.notna(val):
                    safe_col = self._safe_property(col)
                    row_dict[safe_col] = val
            rows.append(row_dict)
        
        with self.driver.session() as session:
            query = f"""
                UNWIND $rows as row
                MERGE (n:`{safe_label}` {{ `{safe_pk}`: row.`{safe_pk}` }})
                SET n += row
            """
            session.run(query, rows=rows)
            
            # Count nodes
            count_result = session.run(f"MATCH (n:`{safe_label}`) RETURN count(n) as count")
            node_count = count_result.single()["count"]
            
            # Create constraint
            constraint_name = f"unique_{safe_label}_{safe_pk}"
            session.run(f"""
                CREATE CONSTRAINT {constraint_name} 
                IF NOT EXISTS 
                FOR (n:`{safe_label}`) 
                REQUIRE n.`{safe_pk}` IS UNIQUE
            """)
            
            print(f"    ✅ {node_count} {safe_label} nodes created")
            return node_count
    
    def _create_all_fk_relationships(self, db_path: str, table_nodes: Dict, schemas: Dict) -> int:
        """🔥 CREATE ALL FOREIGN KEY RELATIONSHIPS"""
        print("  🔗🔥 DETECTING & CREATING FK RELATIONSHIPS...")
        total_rels = 0
        
        conn = sqlite3.connect(db_path)
        
        for source_table, source_info in table_nodes.items():
            source_label = source_info['label']
            source_pk = source_info['pk']
            
            print(f"    🔍 Analyzing {source_table} for FKs...")
            source_df = pd.read_sql_query(f"SELECT * FROM {source_table}", conn)
            
            for target_table, target_info in table_nodes.items():
                if source_table == target_table:
                    continue
                
                target_pk = target_info['pk']
                target_schema = schemas[target_table]  # ✅ FIXED: Available here
                
                # 🔥 IMPROVED FK DETECTION
                fk_col = self._detect_fk_to_target(source_df, target_table, target_pk, conn)
                
                if fk_col:
                    rel_count = self._create_fk_rels(
                        conn, source_label, source_pk, target_info['label'], 
                        target_pk, source_table, fk_col
                    )
                    total_rels += rel_count
                    print(f"    ✅ {source_table}.{fk_col} → {target_table}: {rel_count} rels ✓")
        
        conn.close()
        return total_rels
    
    def _detect_fk_to_target(self, source_df: pd.DataFrame, target_table: str, target_pk: str, conn: sqlite3.Connection) -> str:
        """🔥 ROBUST FK DETECTION"""
        # 1. Exact FK patterns
        fk_patterns = [
            f'{target_table.lower()}_id',
            f'{target_table.lower()}_ID',
            target_pk.lower()
        ]
        
        for col in source_df.columns:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in fk_patterns):
                # 2. Verify by value matching
                unique_vals = source_df[col].dropna().unique()
                if len(unique_vals) == 0:
                    continue
                
                # Sample check (first 10 values)
                sample_vals = unique_vals[:10]
                check_query = f"""
                    SELECT COUNT(*) as match_count 
                    FROM {target_table} 
                    WHERE {target_pk} IN ({','.join(map(str, sample_vals))})
                """
                
                try:
                    match_count = pd.read_sql_query(check_query, conn)['match_count'][0]
                    match_ratio = match_count / len(sample_vals)
                    
                    if match_ratio > 0.3:  # At least 30% match
                        print(f"      �� FK DETECTED: {col} → {target_table}.{target_pk} ({match_ratio:.1%})")
                        return col
                except:
                    continue
        
        # 3. Fallback: columns with few unique values (likely FK)
        for col in source_df.columns:
            unique_count = source_df[col].nunique()
            total_rows = len(source_df)
            
            if unique_count < total_rows * 0.1 and unique_count > 1:  # Likely FK
                # Quick check if values exist in target
                sample_vals = source_df[col].dropna().unique()[:5]
                if len(sample_vals) > 0:
                    check_query = f"""
                        SELECT COUNT(*) as match_count 
                        FROM {target_table} 
                        WHERE {target_pk} IN ({','.join(map(str, sample_vals))})
                    """
                    try:
                        match_count = pd.read_sql_query(check_query, conn)['match_count'][0]
                        if match_count > 0:
                            print(f"      🔗 FK INFERRED: {col} → {target_table} (inferred)")
                            return col
                    except:
                        continue
        
        return None
    
    def _create_fk_rels(self, conn: sqlite3.Connection, source_label: str, source_pk: str, 
                       target_label: str, target_pk: str, source_table: str, fk_col: str) -> int:
        """Create FK relationships"""
        rel_type = f"REFERENCES_{self._safe_label(fk_col.upper())}"
        
        df = pd.read_sql_query(f"SELECT * FROM {source_table} WHERE {fk_col} IS NOT NULL", conn)
        
        created_count = 0
        with self.driver.session() as session:
            for _, row in df.iterrows():
                source_id = row[source_pk]
                target_id = row[fk_col]
                
                if pd.notna(target_id):
                    try:
                        session.run(f"""
                            MATCH (source:`{source_label}` {{ `{self._safe_property(source_pk)}`: $source_id }})
                            MATCH (target:`{target_label}` {{ `{self._safe_property(target_pk)}`: $target_id }})
                            MERGE (source)-[:`{rel_type}`]->(target)
                        """, source_id=source_id, target_id=target_id)
                        created_count += 1
                    except Exception as e:
                        print(f"      ⚠️  Rel creation error: {e}")
                        continue
        
        return created_count
    
    def _create_categorical_relationships(self, db_path: str, table_nodes: Dict, schemas: Dict):
        """Create Category relationships"""
        print("  🏷️  Creating categorical relationships...")
        
        categorical_cols = ['department', 'city', 'category', 'status', 'type', 'name', 'product', 'country', 'genre', 'title']
        
        for table_name, table_info in table_nodes.items():
            safe_label = table_info['label']
            pk = table_info['pk']
            
            conn = sqlite3.connect(db_path)
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            conn.close()
            
            for col in categorical_cols:
                if col in df.columns:
                    rel_name = f"HAS_{self._safe_property(col).upper()}"
                    unique_cats = df[col].dropna().unique()
                    
                    with self.driver.session() as session:
                        for cat_value in unique_cats[:20]:  # Limit
                            if str(cat_value).strip():
                                session.run(f"""
                                    MATCH (n:`{safe_label}`)
                                    WHERE n.`{self._safe_property(col)}` = $cat_value
                                    MERGE (cat:Category {{ name: $cat_name }})
                                    MERGE (n)-[:`{rel_name}`]->(cat)
                                """, cat_value=str(cat_value), cat_name=str(cat_value))
                    
                    print(f"    ✅ {table_name}.{col} → Category ({len(unique_cats)})")
                    break  # One per table
        
        print("    ✅ Categorical relationships complete")
    
    def _safe_cleanup(self):
        """Safe Neo4j cleanup"""
        print("  🧹 Cleaning Neo4j...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            try:
                constraints = session.run("""
                    CALL db.constraints()
                    YIELD name
                    RETURN name
                """).data()
                for constraint in constraints:
                    session.run(f"DROP CONSTRAINT {constraint['name']} IF EXISTS")
            except:
                pass
    
    def _safe_label(self, name: str) -> str:
        return re.sub(r'[^a-zA-Z0-9_]', '_', name)[:63]
    
    def _safe_property(self, name: str) -> str:
        return re.sub(r'[^a-zA-Z0-9_]', '_', name)[:63]
    
    def close(self):
        self.driver.close()
