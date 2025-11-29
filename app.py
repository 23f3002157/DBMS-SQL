import streamlit as st
import pandas as pd
import os
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase
import plotly.express as px
import plotly.graph_objects as go
import traceback

# Dynamic imports
from src.ingestion.dynamic_ingestor import DynamicIngestor
from src.kg_converter.dynamic_converter import DynamicKGConverter
from src.query_layer.llm_orchestrator import DualPathOrchestrator
from src.llm.client import LLMClient
from src.visualization.kg_visualizer import KGVisualizer

load_dotenv()

st.set_page_config(page_title="🔗 DBMS_SQL: Dual Path KG + LLM", layout="wide", page_icon="🔗")

@st.cache_resource
def get_neo4j():
    return GraphDatabase.driver(os.getenv("NEO4J_URI"), 
                               auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")))

st.title("🔗 **DBMS_SQL: Dual Path Knowledge Graph + LLM**")
st.markdown("**🗄️ SQL + 🌐 KG → Both Solutions Always Available**")

tab1, tab2, tab3, tab4 = st.tabs(["📁 Upload & Convert", "🗺️ Visualize KG", "🤖 Dual Path LLM", "📊 Schema"])

# ... [TABS 1-4 remain exactly the same as previous version] ...

with tab1:
    st.header("📤 **Upload Your SQLite Database**")
    
    uploaded_file = st.file_uploader("Choose SQLite file", type="db")
    
    if uploaded_file is not None:
        with open("temp_uploaded.db", "wb") as f:
            f.write(uploaded_file.getvalue())
        st.success(f"✅ {uploaded_file.name} uploaded ({os.path.getsize('temp_uploaded.db')} bytes)")
        
        if st.button("🔍 **ANALYZE DATABASE**", type="primary", use_container_width=True):
            with st.spinner("Analyzing tables..."):
                try:
                    ingestor = DynamicIngestor("temp_uploaded.db")
                    schema_info = ingestor.ingest_all_tables()
                    
                    st.session_state.schema_info = schema_info
                    st.session_state.db_path = "temp_uploaded.db"
                    
                    st.success(f"✅ {len(schema_info['tables'])} tables analyzed")
                    
                    st.subheader("📋 Database Summary")
                    tables_df = pd.DataFrame([
                        {
                            'Table': table,
                            'Rows': f"{schema_info['schemas'][table]['row_count']:,}",
                            'Columns': len(schema_info['schemas'][table]['columns']),
                            'Primary Key': schema_info['schemas'][table]['primary_key']
                        }
                        for table in schema_info['tables']
                    ])
                    st.dataframe(tables_df, use_container_width=True)
                    
                except Exception as e:
                    st.error(f"❌ Analysis failed: {str(e)}")
        
        if st.session_state.get('schema_info', None):
            if st.button("🔥 **CREATE KNOWLEDGE GRAPH**", type="primary", use_container_width=True):
                with st.spinner("Building knowledge graph..."):
                    try:
                        converter = DynamicKGConverter()
                        schema_info = converter.convert_all_tables("temp_uploaded.db")
                        converter.close()
                        
                        st.session_state.kg_created = True
                        st.session_state.schema_info = schema_info
                        
                        st.success("✅ Knowledge graph created")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Conversion failed: {str(e)}")
        else:
            st.info("👆 Click ANALYZE DATABASE first")
    else:
        st.info("👆 Upload a SQLite (.db) file")

with tab2:
    st.header("🗺️ Knowledge Graph")
    
    if st.session_state.get('kg_created', False):
        try:
            driver = get_neo4j()
            with driver.session() as session:
                stats_result = session.run("""
                    MATCH (n) 
                    RETURN labels(n)[0] as label, count(n) as nodes
                    ORDER BY nodes DESC
                    LIMIT 10
                """)
                stats = [dict(r) for r in stats_result]
                
                rel_result = session.run("""
                    MATCH ()-[r]->() 
                    RETURN type(r) as label, count(r) as relationships
                    ORDER BY relationships DESC
                    LIMIT 10
                """)
                rels = [dict(r) for r in rel_result]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                total_nodes = sum(s['nodes'] for s in stats)
                st.metric("Nodes", f"{total_nodes:,}")
            with col2:
                total_rels = sum(r['relationships'] for r in rels)
                st.metric("Relationships", f"{total_rels:,}")
            with col3:
                top_entity = stats[0]['label'] if stats else "None"
                st.metric("Top Entity", top_entity)
            
            st.subheader("KG Structure")
            all_stats = stats + [{"label": r['label'], "nodes": 0, "relationships": r['relationships']} for r in rels]
            st.dataframe(pd.DataFrame(all_stats), use_container_width=True)
            
            st.markdown("[🔗 Open Neo4j Browser](http://localhost:7474)")
            
        except Exception as e:
            st.error(f"❌ Visualization error: {str(e)}")
    else:
        st.warning("👆 Convert database first")

with tab3:
    st.header("🤖 Dual Path LLM Query")
    st.markdown("**🗄️ SQL Solution | 🌐 KG Solution | Both Always Available**")
    
    if st.session_state.get('kg_created', False):
        question = st.text_area(
            "Ask any question:", 
            placeholder="Show me all car models, top customers, sales by region, etc.",
            height=80
        )
        
        if st.button("🚀 **RUN DUAL PATH QUERY**", type="primary", use_container_width=True):
            with st.spinner("Running both SQL and KG queries..."):
                try:
                    orchestrator = DualPathOrchestrator(
                        get_neo4j(), 
                        st.session_state.db_path
                    )
                    
                    result = orchestrator.dual_path_query(question)
                    
                    st.markdown("### 🗄️ SQL Solution")
                    sql_path = result['sql_path']
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.info(f"Rows returned: {sql_path.get('row_count', 0)}")
                        st.code(sql_path.get('query', 'N/A'), language="sql")
                    
                    with col2:
                        st.markdown("**SQL Answer:**")
                        st.write(sql_path.get('answer', 'No answer generated'))
                        
                        if sql_path.get('data'):
                            with st.expander("View SQL Data"):
                                st.dataframe(pd.DataFrame(sql_path['data']))
                    
                    st.markdown("---")
                    st.markdown("### 🌐 Knowledge Graph Solution")
                    cypher_path = result['cypher_path']
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.info(f"Results returned: {cypher_path.get('result_count', 0)}")
                        st.code(cypher_path.get('query', 'N/A'), language="cypher")
                    
                    with col2:
                        st.markdown("**KG Answer:**")
                        st.write(cypher_path.get('answer', 'No answer generated'))
                        
                        if cypher_path.get('data'):
                            with st.expander("View KG Data"):
                                st.json(cypher_path['data'][:10])
                    
                    st.markdown("---")
                    # st.markdown("### 📋 Query Summary")
                    # col1, col2, col3, col4 = st.columns(4)
                    # with col1:
                    #     st.metric("SQL Rows", sql_path.get('row_count', 0))
                    # with col2:
                    #     st.metric("KG Results", cypher_path.get('result_count', 0))
                    # with col3:
                    #     st.metric("SQL Answer", "✅" if sql_path.get('answer') else "❌")
                    # with col4:
                    #     st.metric("KG Answer", "✅" if cypher_path.get('answer') else "❌")
                    
                    st.success("Both solutions complete")
                    
                except Exception as e:
                    st.error(f"❌ Query failed: {str(e)}")
    else:
        st.warning("👆 Convert database first")

with tab4:
    st.header("📊 Database Schema")
    
    if st.session_state.get('schema_info', None):
        for table_name, schema in st.session_state.schema_info['schemas'].items():
            with st.expander(f"{table_name} ({schema['row_count']:,} rows)"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Primary Key:**")
                    st.code(schema['primary_key'])
                    
                    st.markdown("**Columns:**")
                    cols_df = pd.DataFrame(schema['columns'])
                    st.dataframe(cols_df[['name', 'type']], use_container_width=True)
                
                with col2:
                    if schema['sample_data']:
                        st.markdown("**Sample Data:**")
                        sample_df = pd.DataFrame(schema['sample_data'])
                        st.dataframe(sample_df.head(), use_container_width=True)
    else:
        st.info("👆 Upload and analyze database first")

# 🔥 ENHANCED SIDEBAR
with st.sidebar:
    st.markdown("## 🔧 **System Status**")
    
    # Neo4j Status
    try:
        driver = get_neo4j()
        with driver.session() as session:
            node_count = session.run("MATCH (n) RETURN count(n) as count").single()["count"]
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]
        st.success(f"**Neo4j KG**")
        st.metric("Nodes", node_count)
        st.metric("Relationships", rel_count)
    except:
        st.warning("**Neo4j**")
        st.metric("Status", "❌ Disconnected")
    
    st.success("**Dual Path LLM**")
    st.metric("Status", "✅ Ready")
    
    st.markdown("---")
    st.markdown("## 📡 **Quick Links**")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("[🔗 **Neo4j Browser**](http://localhost:7474)")
    with col2:
        st.markdown("[📊 **Sample DBs**](https://github.com/davidjamesknight/SQLite_databases_for_learning_data_science)")
    
    st.markdown("[🚀 **GitHub Repo**](https://github.com/yourusername/DBMS_SQL)")
    st.markdown("[📚 **Documentation**]()")
    
    st.markdown("---")
    st.markdown("## 🎯 **How It Works**")
    st.markdown("""
    **1. Upload** → Any SQLite (.db)  
    **2. Analyze** → Auto-detect tables  
    **3. Convert** → Build KG with relationships  
    **4. Query** → SQL + Cypher → Both answers
    """)
    
    st.markdown("---")
    st.markdown("## 🧪 **Sample Queries**")
    st.markdown("""
    - `Show me all car models`
    - `Top 5 customers by spend`  
    - `What products are most popular?`
    - `Find connected data`
    """)
    
    st.markdown("---")
    st.markdown("## 📦 **Supported DBs**")
    st.markdown("""
    ✅ Airlines • Medical • Sales  
    ✅ Customers • Inventory  
    ✅ **Any SQLite database**
    """)
    
    st.markdown("---")
    st.markdown("*🔗 **DBMS_SQL v2.0** | Dual Path Intelligence*")

st.markdown("---")
st.markdown("*DBMS_SQL | Dual Path SQL + KG | Always Answers*")
