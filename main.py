import os
import sqlite3
from dotenv import load_dotenv
from neo4j import GraphDatabase
from src.kg_converter.table_to_kg import TableToKGConverter
from src.llm.client import LLMClient
from src.query_layer.llm_orchestrator import LLMOrchestrator
import json

load_dotenv()

def ensure_sample_db():
    """Create sample DB only if it doesn't exist"""
    db_path = "data/sample_databases/employees.db"
    
    # Check if DB exists and has data
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
        conn.close()
        if count > 0:
            print(f"✅ Sample DB exists with {count} employees - SKIPPING recreation")
            return
    
    # Create if missing
    from create_sample import create_sample
    create_sample()

def run_full_pipeline():
    """End-to-end: SQLite → KG → LLM Evaluation"""
    print("🚀 DBMS_SQL Pipeline Starting...")
    
    # Config
    db_path = "data/sample_databases/employees.db"
    table_name = "employees"
    
    # 1. Ensure sample data exists
    ensure_sample_db()
    
    # 2. Convert to KG
    print("🔄 Converting SQLite → Knowledge Graph...")
    converter = TableToKGConverter()
    converter.convert(db_path, table_name)
    converter.close()
    print("✅ KG Created!")
    
    # 3. Setup LLM Orchestrator (AIPipe)
    llm_client = LLMClient()
    neo4j_driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"), 
        auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
    )
    
    # 4. Test questions
    questions = [
        "Who has the highest salary?",
        "Who manages the most employees?",
        "Show engineering team salaries"
    ]
    
    kg_results = []
    for question in questions:
        print(f"\n❓ {question}")
        
        # KG + LLM (AIPipe)
        try:
            orchestrator = LLMOrchestrator(neo4j_driver, db_path, llm_client)
            kg_result = orchestrator.answer_kg_pipeline(question)
            kg_results.append(kg_result)
            print(f"   ✅ KG+LLM: {kg_result['final_answer'][:60]}...")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "="*60)
    print("🎉 PIPELINE SUCCESS!")
    print(f"✅ Processed {len(kg_results)} questions via AIPipe")
    print(f"🔗 API: https://aipipe.org/openai/v1")
    
    neo4j_driver.close()

if __name__ == "__main__":
    run_full_pipeline()