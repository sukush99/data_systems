from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Literal, Optional

class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="conn.env", env_file_encoding="utf-8"
    )

    account_storage: str
    #azure_sql_connection_string: str
    azure_storage_connection_string: str
    username_azure: str
    password_azure: str 
    database_name: str
    server_name: str
    container_name: str


config = Config()