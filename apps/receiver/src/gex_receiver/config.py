from pydantic_settings import SettingsConfigDict

from gex_common.config import AppSettings as BaseSettings


class AppSettings(BaseSettings):
    max_request_size_kb: int = 1024

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


APP_SETTINGS = AppSettings()
