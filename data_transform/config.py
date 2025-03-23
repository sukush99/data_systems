from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Literal, Optional


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="settings.env", env_file_encoding="utf-8"
    )
    container_name: str
    file_list: List[str]
    company_list: List[str]
    connection_string: str
    search_container_name: str


config = Config()