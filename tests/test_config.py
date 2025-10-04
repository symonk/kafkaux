import pathlib

import pytest

from kafkaux.config.config import DEFAULT_CFG_ENV_VAR
from kafkaux.config.config import load_configuration


def test_default_config_is_loaded(
    fx_valid_cfg_bootstrap: pathlib.Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        "kafkaux.config.config.DEFAULT_CONFIG_DIR", str(fx_valid_cfg_bootstrap)
    )
    cfg = load_configuration()
    assert cfg.librdkafka == {
        "bootstrap.servers": "localhost:9092,localhost:9092",
        "foo": "bar",
        "hello": "world",
    }


def test_config_override_in_env_var_is_looked_up(
    fx_valid_cfg_bootstrap: pathlib.Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv(DEFAULT_CFG_ENV_VAR, str(fx_valid_cfg_bootstrap))
    cfg = load_configuration(str(fx_valid_cfg_bootstrap))
    assert cfg.librdkafka == {
        "bootstrap.servers": "localhost:9092,localhost:9092",
        "foo": "bar",
        "hello": "world",
    }


def test_user_provided_takes_precedence_over_env_var(
    monkeypatch: pytest.MonkeyPatch, fx_valid_cfg_bootstrap: pathlib.Path
):
    monkeypatch.setenv(DEFAULT_CFG_ENV_VAR, "does_not_exist.ini")
    cfg = load_configuration(str(fx_valid_cfg_bootstrap))
    assert cfg.librdkafka == {
        "bootstrap.servers": "localhost:9092,localhost:9092",
        "foo": "bar",
        "hello": "world",
    }
