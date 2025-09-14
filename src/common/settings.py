from pydantic_settings import BaseSettings, SettingsConfigDict

class BaseAppSettings(BaseSettings):
    KAFKA_BROKERS: str = "localhost:19092"
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
