import os
import requests
import json
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class LLMClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None, model: str = "gpt-4o-mini"):
        self.base_url = base_url or os.getenv("AIPIPE_BASE_URL", "https://api.openai.com/v1")
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def _make_request(self, endpoint: str, payload: Dict) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        response = requests.post(url, headers=self.headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def generate_sql(self, question: str, schema: str) -> str:
        payload = {
            "model": self.model,
            "messages": [{
                "role": "user", 
                "content": f"""Generate SQL query:

SCHEMA:
{schema}

Question: {question}

Return ONLY the SQL query:"""
            }],
            "max_tokens": 400,
            "temperature": 0.1
        }
        result = self._make_request("chat/completions", payload)
        sql = result["choices"][0]["message"]["content"].strip()
        return sql.replace("```sql", "").replace("```", "").strip()
    
    def translate_to_cypher(self, question: str, schema: str) -> str:
        payload = {
            "model": self.model,
            "messages": [{
                "role": "system", 
                "content": f"Neo4j Cypher expert. Schema: {schema}\nAlways use LIMIT."
            }, {"role": "user", "content": question}],
            "max_tokens": 500,
            "temperature": 0.1
        }
        result = self._make_request("chat/completions", payload)
        cypher = result["choices"][0]["message"]["content"].strip()
        return cypher.replace("```cypher", "").replace("```", "").strip()
    
    def synthesize_sql_answer(self, question: str, data: List, schema: str) -> Dict:
        data_str = json.dumps(data[:10], indent=2) if data else "No data returned"
        payload = {
            "model": self.model,
            "messages": [{
                "role": "user", 
                "content": f"Question: {question}\nSQL Results: {data_str}\n\nAnswer the question directly based on the results:"
            }],
            "max_tokens": 300
        }
        result = self._make_request("chat/completions", payload)
        return {"answer": result["choices"][0]["message"]["content"].strip(), "explanation": ""}
    
    def synthesize_kg_answer(self, question: str, data: List, schema: str) -> Dict:
        data_str = json.dumps(data[:10], indent=2) if data else "No data returned"
        payload = {
            "model": self.model,
            "messages": [{
                "role": "user", 
                "content": f"Question: {question}\nKG Results: {data_str}\n\nAnswer the question directly based on the graph results:"
            }],
            "max_tokens": 300
        }
        result = self._make_request("chat/completions", payload)
        return {"answer": result["choices"][0]["message"]["content"].strip(), "explanation": ""}
    
    def generate_fallback_answer(self, question: str, schema: str, method: str) -> str:
        """ALWAYS generates answer even if query fails"""
        payload = {
            "model": self.model,
            "messages": [{
                "role": "user", 
                "content": f"""Question: {question}

{schema}

Using {method} knowledge only (no query execution), provide the most likely answer.
If you cannot find exact data, give a reasonable summary or say what you'd expect to find.

Answer directly:"""
            }],
            "max_tokens": 300,
            "temperature": 0.3
        }
        result = self._make_request("chat/completions", payload)
        return result["choices"][0]["message"]["content"].strip()
