"""Unit tests for ``gex_worker.config.AppSettings``."""

import pytest

from gex_worker.config import APP_SETTINGS, AppSettings

pytestmark = pytest.mark.unit


class TestAppSettings:
    def test_extends_base_settings(self) -> None:
        """Worker AppSettings inherits base fields from gex_common."""
        settings = AppSettings()
        assert hasattr(settings, "database_url")
        assert hasattr(settings, "rabbitmq_url")
        assert hasattr(settings, "grummer_secret_hex")
        assert hasattr(settings, "webhook_site_url")
        assert hasattr(settings, "sms_failure_rate")
        assert hasattr(settings, "log_level")
        assert hasattr(settings, "environment")

    def test_default_consumer_concurrency(self) -> None:
        assert AppSettings().consumer_concurrency == 1

    def test_module_singleton_is_an_instance(self) -> None:
        assert isinstance(APP_SETTINGS, AppSettings)

    def test_sms_failure_rate_default(self) -> None:
        assert AppSettings().sms_failure_rate == 0.1
