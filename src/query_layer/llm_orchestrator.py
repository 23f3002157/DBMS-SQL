from typing import Dict, List, Any
from neo4j import GraphDatabase
import sqlite3
import pandas as pd
import json
from ..llm.client import LLMClient
from ..ingestion.dynamic_ingestor import DynamicIngestor

class DualPathOrchestrator:
    def __init__(self, neo4j_driver, db_path: str, llm_client=None):
        self.neo4j_driver = neo4j_driver
        self.db_path = db_path
        self.llm = llm_client or LLMClient()
        self.ingestor = DynamicIngestor(db_path)
        self.schema_info = self.ingestor.ingest_all_tables()
    
    def dual_path_query(self, question: str) -> Dict:
        """DUAL PATH: SQL + Cypher - ALWAYS RETURNS ANSWERS"""
        
        # PATH 1: SQL
        sql_result = self._sql_path(question)
        
        # PATH 2: Cypher
        cypher_result = self._cypher_path(question)
        
        return {
            "sql_path": sql_result,
            "cypher_path": cypher_result
        }
    
    def _sql_path(self, question: str) -> Dict:
        """SQL Path - ALWAYS generates answer"""
        schema_text = self._get_sql_schema()
        sql_query = self.llm.generate_sql(question, schema_text)
        
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(sql_query, conn)
            sql_data = df.to_dict('records')
            conn.close()
            
            answer = self.llm.synthesize_sql_answer(question, sql_data, schema_text)
            
            return {
                "method": "SQL",
                "query": sql_query,
                "data": sql_data,
                "row_count": len(sql_data),
                "answer": answer["answer"],
                "explanation": answer["explanation"]
            }
        except Exception as e:
            # FALLBACK: Generate answer from schema even if query fails
            fallback_answer = self.llm.generate_fallback_answer(question, schema_text, "SQL")
            return {
                "method": "SQL",
                "query": sql_query,
                "data": [],
                "row_count": 0,
                "answer": fallback_answer,
                "explanation": f"Query failed: {str(e)} - using schema knowledge"
            }
    
    def _cypher_path(self, question: str) -> Dict:
        """Cypher Path - ALWAYS generates answer"""
        schema_text = self._get_kg_schema()
        cypher_query = self.llm.translate_to_cypher(question, schema_text)
        
        try:
            with self.neo4j_driver.session() as session:
                result = session.run(cypher_query + " LIMIT 50")
                cypher_data = [dict(record) for record in result]
            
            answer = self.llm.synthesize_kg_answer(question, cypher_data, schema_text)
            
            return {
                "method": "Knowledge Graph", 
                "query": cypher_query,
                "data": cypher_data,
                "result_count": len(cypher_data),
                "answer": answer["answer"],
                "explanation": answer["explanation"]
            }
        except Exception as e:
            # FALLBACK: Generate answer from schema even if query fails
            fallback_answer = self.llm.generate_fallback_answer(question, schema_text, "Cypher")
            return {
                "method": "Knowledge Graph",
                "query": cypher_query,
                "data": [],
                "result_count": 0,
                "answer": fallback_answer,
                "explanation": f"Query failed: {str(e)} - using graph schema knowledge"
            }
    
    def _get_sql_schema(self) -> str:
        """SQL Schema"""
        tables = []
        for table, schema in self.schema_info['schemas'].items():
            cols = [c['name'] for c in schema['columns']]
            tables.append(f"Table `{table}`: {', '.join(cols)}")
        return "\n".join(tables)
    
    def _get_kg_schema(self) -> str:
        """KG Schema"""
        try:
            with self.neo4j_driver.session() as session:
                nodes = session.run("MATCH (n) RETURN distinct labels(n)[0] as label").data()
                rels = session.run("MATCH ()-[r]->() RETURN distinct type(r) as type").data()
            node_labels = ', '.join([n['label'] for n in nodes])
            rel_types = ', '.join([r['type'] for r in rels])
            return f"Nodes: {node_labels}\nRelationships: {rel_types}"
        except:
            return "Knowledge Graph with multiple connected entities"
