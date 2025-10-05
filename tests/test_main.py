from typer.testing import CliRunner

from kafkaux.config.config import DEFAULT_CFG_ENV_VAR
from kafkaux.main import app

runner = CliRunner()


def test_unsupported_filter_raises_bad_paramter(fx_valid_cfg_bootstrap):
    env = {DEFAULT_CFG_ENV_VAR: str(fx_valid_cfg_bootstrap)}
    args = (
        "consume",
        "--topics",
        "my-topic",
        "--tail",
        "--filters",
        "key_madeup=^test.*",
    )
    result = runner.invoke(app, args, env=env)
    assert (
        "Invalid value: unsupported --filter key: key_madeup, must be in"
        in result.stderr
    )
    assert result.exit_code == 2
