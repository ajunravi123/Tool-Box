from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Tuple, Dict, Union
import asyncpg
import httpx
import os
import re
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account

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

# Default PostgreSQL database connection configuration
DEFAULT_POSTGRES_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "your_database"),
    "user": os.getenv("DB_USER", "your_user"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
    "port": os.getenv("DB_PORT", "5432")
}

class DBConfig(BaseModel):
    db_type: str  # "postgres" or "bigquery"
    host: Optional[str] = None  # For PostgreSQL
    database: Optional[str] = None  # For PostgreSQL
    user: Optional[str] = None  # For PostgreSQL
    password: Optional[str] = None  # For PostgreSQL
    port: Optional[str] = None  # For PostgreSQL
    project_id: Optional[str] = None  # For BigQuery
    credentials_path: Optional[str] = None  # For BigQuery (path to service account JSON)
    credentials_json: Optional[Dict] = None  # For BigQuery (service account JSON object)

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

async def get_postgres_schema(db_config: Dict[str, str]) -> str:
    """Fetch PostgreSQL database schema for context"""
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
        logger.error(f"Failed to fetch PostgreSQL schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch PostgreSQL schema: {str(e)}")

def get_bigquery_schema(db_config: Dict[str, str]) -> str:
    """Fetch BigQuery dataset schema for context"""
    try:
        project_id = db_config.get("project_id")
        dataset_id = db_config.get("database")
        credentials_path = db_config.get("credentials_path")
        credentials_json = db_config.get("credentials_json")
        
        if not project_id or not dataset_id:
            raise HTTPException(status_code=400, detail="project_id and database (dataset_id) are required for BigQuery")
        
        if credentials_json:
            credentials = service_account.Credentials.from_service_account_info(credentials_json)
            client = bigquery.Client(project=project_id, credentials=credentials)
        elif credentials_path:
            client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
        else:
            client = bigquery.Client(project=project_id)
        
        dataset_ref = client.dataset(dataset_id)
        tables = client.list_tables(dataset_ref)
        
        schema_context = ""
        for table in tables:
            table_ref = dataset_ref.table(table.table_id)
            table = client.get_table(table_ref)
            schema_context += f"\nTable: {table.table_id}\n"
            for field in table.schema:
                schema_context += f"  Column: {field.name} ({field.field_type})\n"
        return schema_context
    except GoogleCloudError as e:
        logger.error(f"Failed to fetch BigQuery schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch BigQuery schema: {str(e)}")
    except Exception as e:
        logger.error(f"BigQuery error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"BigQuery error: {str(e)}")

async def get_db_schema(db_config: Dict[str, str]) -> str:
    """Fetch schema based on database type"""
    db_type = db_config.get("db_type", "postgres")
    if db_type == "postgres":
        return await get_postgres_schema(db_config)
    elif db_type == "bigquery":
        return get_bigquery_schema(db_config)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported db_type: {db_type}")

async def generate_sql_query(natural_query: str, schema_context: str, db_type: str) -> str:
    """Generate SQL query using Gemini API"""
    if not GEMINI_API_KEY or GEMINI_API_KEY == "GEMINI_API_KEY":
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY environment variable is not set or invalid")

    try:
        sql_dialect = "PostgreSQL" if db_type == "postgres" else "BigQuery Standard SQL"
        prompt = f"""
        Convert the following natural language query to a {sql_dialect} SELECT query.
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

async def validate_postgres_query(query: str, db_config: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """Validate PostgreSQL query"""
    try:
        conn = await asyncpg.connect(**db_config)
        await conn.fetch(f"EXPLAIN {query}")
        await conn.close()
        return True, None
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"PostgreSQL query validation failed: {str(e)}")
        return False, str(e)
    except Exception as e:
        logger.error(f"PostgreSQL connection error: {str(e)}")
        return False, f"PostgreSQL connection error: {str(e)}"

def validate_bigquery_query(query: str, db_config: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """Validate BigQuery query using dry run"""
    try:
        project_id = db_config.get("project_id")
        credentials_path = db_config.get("credentials_path")
        credentials_json = db_config.get("credentials_json")
        
        if not project_id:
            raise HTTPException(status_code=400, detail="project_id is required for BigQuery")
        
        if credentials_json:
            credentials = service_account.Credentials.from_service_account_info(credentials_json)
            client = bigquery.Client(project=project_id, credentials=credentials)
        elif credentials_path:
            client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
        else:
            client = bigquery.Client(project=project_id)
        
        job_config = bigquery.QueryJobConfig(dry_run=True)
        client.query(query, job_config=job_config)
        return True, None
    except GoogleCloudError as e:
        logger.error(f"BigQuery query validation failed: {str(e)}")
        return False, str(e)
    except Exception as e:
        logger.error(f"BigQuery error: {str(e)}")
        return False, f"BigQuery error: {str(e)}"

async def validate_query(query: str, db_config: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """Validate query based on database type"""
    db_type = db_config.get("db_type", "postgres")
    if db_type == "postgres":
        return await validate_postgres_query(query, db_config)
    elif db_type == "bigquery":
        return validate_bigquery_query(query, db_config)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported db_type: {db_type}")

async def fetch_postgres_data(query: str, db_config: Dict[str, str]) -> list:
    """Execute PostgreSQL query and return results"""
    try:
        conn = await asyncpg.connect(**db_config)
        rows = await conn.fetch(query)
        await conn.close()
        return [dict(row) for row in rows]
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"PostgreSQL query execution failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"PostgreSQL query execution failed: {str(e)}")
    except Exception as e:
        logger.error(f"PostgreSQL connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"PostgreSQL connection error: {str(e)}")

def fetch_bigquery_data(query: str, db_config: Dict[str, str]) -> list:
    """Execute BigQuery query and return results"""
    try:
        project_id = db_config.get("project_id")
        credentials_path = db_config.get("credentials_path")
        credentials_json = db_config.get("credentials_json")
        
        if not project_id:
            raise HTTPException(status_code=400, detail="project_id is required for BigQuery")
        
        if credentials_json:
            credentials = service_account.Credentials.from_service_account_info(credentials_json)
            client = bigquery.Client(project=project_id, credentials=credentials)
        elif credentials_path:
            client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
        else:
            client = bigquery.Client(project=project_id)
        
        query_job = client.query(query)
        rows = query_job.result()
        return [dict(row) for row in rows]
    except GoogleCloudError as e:
        logger.error(f"BigQuery query execution failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"BigQuery query execution failed: {str(e)}")
    except Exception as e:
        logger.error(f"BigQuery error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"BigQuery error: {str(e)}")

async def fetch_query_data(query: str, db_config: Dict[str, str]) -> list:
    """Fetch data based on database type"""
    db_type = db_config.get("db_type", "postgres")
    if db_type == "postgres":
        return await fetch_postgres_data(query, db_config)
    elif db_type == "bigquery":
        return fetch_bigquery_data(query, db_config)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported db_type: {db_type}")

@app.post("/generate_query", summary="Generate SQL query from natural language")
async def generate_query(query_input: QueryInput):
    """Generate and validate SQL query"""
    try:
        # Use provided DB config or default
        db_config = query_input.db_config.dict(exclude_unset=True) if query_input.db_config else DEFAULT_POSTGRES_CONFIG
        if not db_config.get("db_type"):
            db_config["db_type"] = "postgres"
        
        # Get schema context from input or database
        schema_context = query_input.schema_context
        if not schema_context:
            schema_context = await get_db_schema(db_config)
        
        # Generate SQL query
        sql_query = await generate_sql_query(query_input.natural_language_query, schema_context, db_config["db_type"])
        
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
        db_config = query_input.db_config.dict(exclude_unset=True) if query_input.db_config else DEFAULT_POSTGRES_CONFIG
        if not db_config.get("db_type"):
            db_config["db_type"] = "postgres"
        
        # Get schema context from input or database
        schema_context = query_input.schema_context
        if not schema_context:
            schema_context = await get_db_schema(db_config)
        
        # Generate SQL query
        sql_query = await generate_sql_query(query_input.natural_language_query, schema_context, db_config["db_type"])
        
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