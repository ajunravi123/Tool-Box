from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Tuple, Dict
import asyncpg
import httpx
import os
import re
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI()

# Gemini API configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "GEMINI_API_KEY")
GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
MODEL_NAME = "gemini-2.0-flash"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default database connection configuration
DEFAULT_DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "your_database"),
    "user": os.getenv("DB_USER", "your_user"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
    "port": os.getenv("DB_PORT", "5432")
}

class DBConfig(BaseModel):
    host: str
    database: str
    user: str
    password: str
    port: str

class QueryInput(BaseModel):
    natural_language_query: str
    schema_context: Optional[str] = None
    db_config: Optional[DBConfig] = None

def clean_sql_query(query: str) -> str:
    """Remove Markdown code block formatting, newlines, and normalize query"""
    cleaned_query = re.sub(r'```sql\s*|\s*```', '', query, flags=re.IGNORECASE)
    cleaned_query = re.sub(r';\s*$', '', cleaned_query)
    cleaned_query = re.sub(r'\s+', ' ', cleaned_query)
    return cleaned_query.strip()

async def get_db_schema(db_config: Dict[str, str]) -> str:
    """Fetch database schema for context using provided DB config"""
    try:
        conn = await asyncpg.connect(**db_config)
        schema_query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        """
        rows = await conn.fetch(schema_query)
        await conn.close()
        
        schema_context = ""
        current_table = None
        for row in rows:
            table = row['table_name']
            if table != current_table:
                schema_context += f"\nTable: {table}\n"
                current_table = table
            schema_context += f"  Column: {row['column_name']} ({row['data_type']})\n"
        return schema_context
    except Exception as e:
        logger.error(f"Failed to fetch schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch schema: {str(e)}")

async def generate_sql_query(natural_query: str, schema_context: str) -> str:
    """Generate SQL query using Gemini API"""
    if not GEMINI_API_KEY or GEMINI_API_KEY == "GEMINI_API_KEY":
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY environment variable is not set or invalid")

    try:
        prompt = f"""
        Convert the following natural language query to a PostgreSQL SELECT query.
        Query: {natural_query}
        Schema: {schema_context}
        Provide only the SQL query as output, without any explanation or Markdown formatting.
        """
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}",
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{
                        "parts": [{"text": prompt}]
                    }],
                    "generationConfig": {
                        "maxOutputTokens": 200,
                        "temperature": 0.7
                    }
                }
            )
            response.raise_for_status()
            result = response.json()
            
            sql_query = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            if not sql_query:
                raise ValueError("Empty query generated")
            
            cleaned_query = clean_sql_query(sql_query)
            logger.info(f"Generated SQL query: {cleaned_query}")
            return cleaned_query
    except httpx.HTTPStatusError as e:
        logger.error(f"Gemini API error: {e.response.text}")
        raise HTTPException(status_code=500, detail=f"Gemini API request failed: {e.response.text}")
    except Exception as e:
        logger.error(f"Query generation error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query generation failed: {str(e)}")

async def validate_query(query: str, db_config: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """Validate generated SQL query and return error message if invalid"""
    try:
        conn = await asyncpg.connect(**db_config)
        await conn.fetch(f"EXPLAIN {query}")
        await conn.close()
        return True, None
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Query validation failed: {str(e)}")
        return False, str(e)
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return False, f"Database connection error: {str(e)}"

async def fetch_query_data(query: str, db_config: Dict[str, str]) -> list:
    """Execute the SQL query and return results as a list of dictionaries"""
    try:
        conn = await asyncpg.connect(**db_config)
        rows = await conn.fetch(query)
        await conn.close()
        
        # Convert rows to list of dictionaries
        results = [dict(row) for row in rows]
        return results
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Query execution failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

@app.post("/generate_query", summary="Generate SQL query from natural language")
async def generate_query(query_input: QueryInput):
    """Generate and validate SQL query"""
    try:
        # Use provided DB config or default
        db_config = query_input.db_config.dict() if query_input.db_config else DEFAULT_DB_CONFIG
        
        # Get schema context from input or database
        schema_context = query_input.schema_context
        if not schema_context:
            schema_context = await get_db_schema(db_config)
        
        # Generate SQL query
        sql_query = await generate_sql_query(query_input.natural_language_query, schema_context)
        
        # Validate generated query
        is_valid, error_message = await validate_query(sql_query, db_config)
        if not is_valid:
            logger.error(f"Invalid query: {sql_query}, Error: {error_message}")
            raise HTTPException(status_code=400, detail=f"Error: {error_message} | Generated query: {sql_query}")
        
        return {"query": sql_query}
    
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.post("/fetch_data", summary="Generate SQL query and fetch data from database")
async def fetch_data(query_input: QueryInput):
    """Generate SQL query, validate, and fetch data from database"""
    try:
        # Use provided DB config or default
        db_config = query_input.db_config.dict() if query_input.db_config else DEFAULT_DB_CONFIG
        
        # Get schema context from input or database
        schema_context = query_input.schema_context
        if not schema_context:
            schema_context = await get_db_schema(db_config)
        
        # Generate SQL query
        sql_query = await generate_sql_query(query_input.natural_language_query, schema_context)
        
        # Validate generated query
        is_valid, error_message = await validate_query(sql_query, db_config)
        if not is_valid:
            logger.error(f"Invalid query: {sql_query}, Error: {error_message}")
            raise HTTPException(status_code=400, detail=f"Error: {error_message} | Generated query: {sql_query}")
        
        # Fetch data
        data = await fetch_query_data(sql_query, db_config)
        
        return {"query": sql_query, "data": data}
    
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")