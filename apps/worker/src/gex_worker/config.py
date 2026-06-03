from gex_common.config import AppSettings as BaseSettings


class AppSettings(BaseSettings):
    consumer_concurrency: int = 1


APP_SETTINGS = AppSettings()
