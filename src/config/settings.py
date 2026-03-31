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

    # Logging
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
