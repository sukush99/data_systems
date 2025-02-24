from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Literal, Optional

class Config(BaseSettings):
    model_congif = SettingsConfigDict(
        env_file="settings.env", env_file_encoding="utf-8"
    )

    company_list: List[str]
    data_source: Literal["live", "backfill"]
    api_access_key: str
    limit: Optional[int] = None
    offsets: Optional[List[int]] = None

config = Config()