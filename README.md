# DBMS_SQL: Dual Path Knowledge Graph + LLM Query Engine

[![Streamlit App](https://img.shields.io/badge/Streamlit-App-brightgreen)](http://localhost:8501)
[![Neo4j](https://img.shields.io/badge/Neo4j-Graph%20Database-yellow)](http://localhost:7474)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

**DBMS_SQL** converts any SQLite database into a fully connected Knowledge Graph and provides dual-path LLM-powered querying (SQL + Cypher). Upload any `.db` file and ask questions in natural language - get answers from both the original database and the semantic graph.

## Features

- **Universal SQLite Support**: Works with any `.db` file (airlines, medical, sales, etc.)
- **Automatic Schema Detection**: Primary keys, table relationships, data types
- **Full Knowledge Graph Conversion**: Nodes + Foreign Key relationships + Categorical connections
- **Dual Path Querying**: SQL (original DB) + Cypher (KG) - both always available
- **LLM-Powered Answers**: Natural language questions → Structured queries → Natural language answers
- **Interactive Visualization**: Neo4j Browser integration + graph metrics
- **Production Ready**: Error handling, fallbacks, clean UI

## Architecture

```
SQLite DB → DynamicIngestor → DynamicKGConverter → Neo4j KG
                                            ↓
                                      DualPathOrchestrator (SQL, Cypher Queries)
                                          
```

## Quick Start

### 1. Prerequisites

```bash
# Clone repository
git clone https://github.com/23f3002157/DBMS-SQL.git
cd DBMS-SQL

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate    # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Neo4j

```bash
# Download & start Neo4j Desktop or use Docker
docker run \
    --name neo4j \
    -p 7474:7474 -p 7687:7687 \
    -d \
    -v $HOME/neo4j/data:/data \
    -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/import \
    -v $HOME/neo4j/plugins:/plugins \
    --env NEO4J_AUTH=neo4j/password \
    neo4j:latest

# Or use the provided start.sh script
chmod +x start.sh
./start.sh
```

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Neo4j credentials and OpenAI API key
```

### 4. Launch Application

```bash
streamlit run app.py
```



## Technical Details

### Knowledge Graph Construction

1. **Node Creation**: Each table → Neo4j node label
2. **Property Mapping**: All columns → node properties
3. **FK Detection**: Automatic foreign key inference
4. **Relationship Creation**: `REFERENCES_*` relationship types
5. **Categorical Links**: `HAS_*` → `Category` nodes

### Dual Path Query Engine

| Path | Query Language | Strengths | Fallback |
|------|----------------|-----------|----------|
| **SQL** | SQLite SQL | Exact tabular data | Schema-based reasoning |
| **Cypher** | Neo4j Cypher | Relationships, patterns | Graph schema reasoning |

### LLM Integration

- **Model**: GPT-4o-mini (configurable)
- **Query Generation**: SQL + Cypher translation
- **Answer Synthesis**: Natural language summaries
- **Fallback Answers**: Always generates response

## Configuration

### `.env` File

```env
# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# LLM (OpenAI)
OPENAI_API_KEY=your_openai_key_here

# Optional: Custom LLM endpoint
AIPIPE_BASE_URL=https://api.openai.com/v1
```

## Directory Structure

```
DBMS_SQL/
├── app.py                    # Streamlit UI
├── src/
│   ├── ingestion/           # SQLite schema analysis
│   ├── kg_converter/        # Neo4j KG construction
│   ├── query_layer/         # Dual path LLM orchestrator
│   ├── llm/                # LLM client
│   └── visualization/       # Graph visualization
├── requirements.txt         # Dependencies
├── start.sh                # Neo4j startup
└── README.md
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `Neo4j connection failed` | Run `./start.sh` or check Docker |
| `LLM API errors` | Verify `OPENAI_API_KEY` in `.env` |
| `No relationships created` | Check FK column naming conventions |
| `Cypher syntax errors` | LLM fallback provides schema-based answer |

### Debug Mode

Add to `.env`:
```env
DEBUG=True
```



---
