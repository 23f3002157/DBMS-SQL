#!/bin/bash
cd "$(dirname "$0")"
echo "🚀 Starting DBMS_SQL Pipeline..."
streamlit run app.py --server.port 8501 --server.address 0.0.0.0