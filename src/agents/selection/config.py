"""
Selection Agent Configuration
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class SelectionSettings(BaseSettings):
    """Selection Agent settings"""
    AGENT_NAME: str = "selection"
    DEMO_SELECTION_SHORTCUT: bool = False  # Enable demo shortcut keyword
    WELCOME_VIDEO_URL: str = "https://example.com/welcome.mp4"  # Fallback video URL
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = SelectionSettings()

