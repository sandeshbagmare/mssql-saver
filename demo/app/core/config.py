"""Application settings loaded from environment / .env file."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    pg_dsn: str = "postgresql://postgres:postgres@localhost:5432/langgraph"
    mssql_conn_str: str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;DATABASE=langgraph;"
        "UID=sa;PWD=SqlPass123!;"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    azure_sql_conn_str: str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;DATABASE=langgraph_azure;"
        "UID=sa;PWD=SqlPass123!;"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    pool_size: int = 10
    app_title: str = "LangGraph MSSQL/Postgres/Azure SQL Demo"


settings = Settings()
