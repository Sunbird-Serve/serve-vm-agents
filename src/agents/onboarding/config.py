from pydantic_settings import BaseSettings, SettingsConfigDict  # <-- import these

class Settings(BaseSettings):
    KAFKA_BROKERS: str = "localhost:19092"
    TOPIC_WA_IN: str = "serve.vm.whatsapp.in"
    TOPIC_WA_OUT: str = "serve.vm.whatsapp.out"
    GROUP_ID: str = "vm-agent-onboarding"
    AGENT_NAME: str = "onboarding"
    MCP_BASE: str = "http://localhost:9000"
    PORT: int = 8001
    # allow unrelated env vars (e.g., AGENT_NAME) without error
    model_config = SettingsConfigDict(env_file=".env",
                                      env_file_encoding="utf-8",
                                      extra="ignore")

settings = Settings()
