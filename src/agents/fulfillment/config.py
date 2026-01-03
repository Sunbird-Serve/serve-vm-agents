"""
Fulfillment Agent Configuration
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class FulfillmentSettings(BaseSettings):
    """Fulfillment Agent settings"""
    AGENT_NAME: str = "fulfillment"
    DEMO_NEEDS_SHORTCUT: bool = False  # Enable demo shortcut keyword
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = FulfillmentSettings()

