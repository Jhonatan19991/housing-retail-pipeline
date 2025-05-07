"""
Database connection module for the real estate data pipeline.

This module provides functionality to create and manage database connections
using SQLAlchemy with PostgreSQL.
"""

from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from typing import Optional

# Load environment variables from .env file
load_dotenv()

def get_engine() -> Optional[create_engine]:
    """
    Create and return a SQLAlchemy engine instance for PostgreSQL connection.
    
    Returns:
        Optional[create_engine]: SQLAlchemy engine instance if successful, None if failed
        
    Environment Variables Required:
        PASSWORD: Database password
        HOST: Database host
        PORT: Database port
        DB: Database name
    """
    try:
        # Database connection parameters
        USERNAME = "postgres"
        PASSWORD = os.getenv('PASSWORD')
        HOST = os.getenv('HOST')
        PORT = os.getenv('PORT')
        DB = os.getenv('DB')

        # Validate required environment variables
        if not all([PASSWORD, HOST, PORT, DB]):
            raise ValueError("Missing required environment variables for database connection")

        # Create database URL
        database_url = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'
        
        # Create and return engine
        return create_engine(database_url)
    
    except Exception as e:
        print(f"Error creating database engine: {str(e)}")
        return None