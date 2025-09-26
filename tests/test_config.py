import pathlib

import pytest

from kafkaux.config.parser import DEFAULT_CFG_ENV_VAR
from kafkaux.config.parser import load_configuration


def test_default_config_is_loaded(fx_simple_config_path): ...


def test_config_override_in_env_var_is_looked_up(): ...


def test_user_provided_takes_precedence_over_env_var(
    monkeypatch: pytest.MonkeyPatch, fx_simple_config_path: pathlib.Path
):
    monkeypatch.setenv(DEFAULT_CFG_ENV_VAR, "does_not_exist.ini")
    cfg = load_configuration(fx_simple_config_path)
    assert cfg.librdkafka == {
        "foo": "bar",
        "hello": "world",
    }
