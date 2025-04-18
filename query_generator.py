from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional, Tuple
import asyncpg
import httpx
import os
import re
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Gemini API configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "GEMINI_API_KEY")
GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
MODEL_NAME = "gemini-2.0-flash"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "your_database"),
    "user": os.getenv("DB_USER", "your_user"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
    "port": os.getenv("DB_PORT", "5432")
}

class QueryInput(BaseModel):
    natural_language_query: str
    schema_context: Optional[str] = None

def clean_sql_query(query: str) -> str:
    """Remove Markdown code block formatting, newlines, and normalize query"""
    # Remove ```sql, ```, and surrounding whitespace
    cleaned_query = re.sub(r'```sql\s*|\s*```', '', query, flags=re.IGNORECASE)
    # Remove trailing semicolon
    cleaned_query = re.sub(r';\s*$', '', cleaned_query)
    # Replace newlines (\n) and multiple spaces with a single space
    cleaned_query = re.sub(r'\s+', ' ', cleaned_query)
    return cleaned_query.strip()

async def get_db_schema():
    """Fetch database schema for context"""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
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
        # Prepare prompt for Gemini
        prompt = f"""
        Convert the following natural language query to a PostgreSQL SELECT query.
        Query: {natural_query}
        Schema: {schema_context}
        Provide only the SQL query as output, without any explanation or Markdown formatting.
        """
        
        # Make API request to Gemini
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
            
            # Extract generated SQL query
            sql_query = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            if not sql_query:
                raise ValueError("Empty query generated")
            
            # Clean the query to remove any Markdown formatting and newlines
            cleaned_query = clean_sql_query(sql_query)
            logger.info(f"Generated SQL query: {cleaned_query}")
            return cleaned_query
    except httpx.HTTPStatusError as e:
        logger.error(f"Gemini API error: {e.response.text}")
        raise HTTPException(status_code=500, detail=f"Gemini API request failed: {e.response.text}")
    except Exception as e:
        logger.error(f"Query generation error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query generation failed: {str(e)}")

async def validate_query(query: str) -> Tuple[bool, Optional[str]]:
    """Validate generated SQL query and return error message if invalid"""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        await conn.fetch(f"EXPLAIN {query}")
        await conn.close()
        return True, None
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Query validation failed: {str(e)}")
        return False, str(e)
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return False, f"Database connection error: {str(e)}"

async def generate_query(query_input: QueryInput):
    """Main function to generate and validate SQL query"""
    try:
        # Get schema context from input or database
        schema_context = query_input.schema_context
        if not schema_context:
            schema_context = await get_db_schema()
        
        # Generate SQL query
        sql_query = await generate_sql_query(query_input.natural_language_query, schema_context)
        
        # Validate generated query
        is_valid, error_message = await validate_query(sql_query)
        if not is_valid:
            logger.error(f"Invalid query: {sql_query}, Error: {error_message}")
            raise HTTPException(status_code=400, detail=f"Error: {error_message} | Generated query: {sql_query}. ")
        
        return {"query": sql_query}
    
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")