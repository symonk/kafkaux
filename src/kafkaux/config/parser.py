import configparser
import os
import pathlib
import typing
from dataclasses import dataclass

# The default (os agnostic) path (~/.config/kafkaux.ini on POSIX)
DEFAULT_CONFIG_DIR = pathlib.Path.home() / ".config" / "kafkaux.ini"

# Env variable that can be set to specify config if --config is not set.
# this should be set to the full qualifying path name to the .ini file
# that contains the configuration.
DEFAULT_CFG_ENV_VAR = "KAFKAUX_CONFIG"


@dataclass(frozen=True)
class Configuration:
    """Configuration encapsulates the runtime configuration of an execution of
    kafkaaux.  Kafkaux allows passing any arbitrary `librdkafka` key:value pairs
    aswell as key value pairs for custom kafkaux behaviour."""

    librdkafka: dict[str, typing.Any]


def load_configuration(user_defined_path: pathlib.Path | None) -> Configuration:
    """load_configuration attempts to parse and load the configuration
    file, typically provided to the `--config` CLI option.

    Config parsing behaves like so:
        - If the user has provided a custom path to a config file, use that explicitly.
        - if no user defined config was provided:
            - check if the KAFKAUX_CONFIG environment variable is set, use it
            - fallback to using ~/.config/kafkaux

    load_configuration raises an exception if the user has provided an explicit path
    but the file does not exist on disk etc, rather than fall back to other lookups
    which can be surprising to the user.
    """
    path_to_check = (
        user_defined_path
        if user_defined_path is not None
        else os.environ.get(DEFAULT_CFG_ENV_VAR) or DEFAULT_CONFIG_DIR
    )
    parser = configparser.ConfigParser()
    parser.read(path_to_check)
    return parse(parser)


def parse(cfg: configparser.ConfigParser) -> Configuration:
    """parse takes a loaded ini config and transforms
    it into the configuration dataclass."""
    return Configuration(librdkafka=get_cfg_section(cfg, "librdkafka"))


def get_cfg_section(
    cfg: configparser.ConfigParser, section: str
) -> dict[str, typing.Any]:
    """get_cfg_section attempts to read an entire section from the parsed config
    object, returning an empty dictionary on failure.  This is useful for obtaining
    a section as a dict, where the section is optional.

    The builtin configparser does not expose a way to lookup sections using the
    .get(..., ...) method."""
    if cfg.has_section(section):
        return dict(cfg[section])
    return {}
