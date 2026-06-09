# inbuilt
from pathlib import Path
import configparser

from handuflow.config.validate import validate_handuflow_config
from handuflow.exception.config_error import ConfigError


def load_config(
    path: str | Path,
    *,
    check_paths_exist: bool = False,
) -> configparser.ConfigParser:
    """
    Load and validate a HanduFlow config.ini file.

    Raises :class:`~handuflow.exception.config_error.ConfigError` if the file is
    missing or invalid.
    """
    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise ConfigError(
            message=f"config.ini not found: {config_path}",
            error_code="HF022",
            details={"hint": "Create config.ini or fix the path passed to load_config()"},
        )

    config = configparser.ConfigParser()
    read_paths = config.read(config_path)
    if not read_paths:
        raise ConfigError(
            message=f"Could not parse config.ini: {config_path}",
            error_code="HF022",
            details={"hint": "Check file permissions and INI syntax"},
        )

    validate_handuflow_config(config, check_paths_exist=check_paths_exist)
    return config
