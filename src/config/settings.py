"""
Parser configuration via environment variables
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database (same as zapchasti-platform)
    database_url: str = "postgresql://ruslan@localhost/zapchasti"

    # Parsing delays (seconds)
    default_delay_min: float = 3.0
    default_delay_max: float = 6.0

    # Concurrency
    max_concurrent_collectors: int = 3

    # LLM (OpenAI-compatible API — DeepSeek or Claude)
    llm_api_key: str = ""
    llm_base_url: str = "https://api.deepseek.com/v1"
    llm_model: str = "deepseek-chat"
    llm_max_tokens: int = 2000
    llm_temperature: float = 0.3

    # Search engine
    search_google_delay_min: float = 30.0
    search_google_delay_max: float = 60.0
    search_site_delay_min: float = 3.0
    search_site_delay_max: float = 5.0
    search_max_results_per_query: int = 10
    search_max_sites_to_parse: int = 30

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
