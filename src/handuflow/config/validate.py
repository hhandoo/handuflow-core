# inbuilt
import os
import configparser

from handuflow.config.config_paths import (
    GLOBAL_VACUUM_HOURS_DEFAULT,
    GLOBAL_VACUUM_HOURS_MAX,
    GLOBAL_VACUUM_HOURS_MIN,
    KEY_GLOBAL_VACUUM_HOURS,
    KEY_SYSTEM_SCHEMA,
    LEGACY_KEY_GLOBAL_VACUUM_HOURS,
    LEGACY_KEY_SYSTEM_SCHEMA,
    _cfg_get_with_legacy,
    cfg_get,
    runtime_mode,
)
from handuflow.exception.config_error import ConfigError

_REQUIRED_DEFAULT = (
    "file_hunt_path",
    "outbound_directory_name",
    "log_directory_name",
)

_REQUIRED_FILES = ("master_spec_name",)

_REQUIRED_LINEAGE = (
    "BOX_WIDTH",
    "BOX_HEIGHT",
    "X_GAP",
    "Y_GAP",
    "ROOT_GAP",
)


def validate_handuflow_config(
    config: configparser.ConfigParser,
    *,
    check_paths_exist: bool = False,
) -> None:
    """
    Validate config.ini before a run. Raises :class:`ConfigError` on fatal issues.

    Parameters
    ----------
    config:
        Parsed config.ini.
    check_paths_exist:
        If True, verify ``file_hunt_path`` and temp directories exist (recommended locally).
    """
    errors: list[str] = []

    if not config.defaults() and not config.sections():
        errors.append(
            "Configuration is empty. Pass a loaded config.ini (config.read() returns "
            "no files if the path is wrong)."
        )

    for key in _REQUIRED_DEFAULT:
        if not cfg_get(config, key):
            errors.append(f"Missing required setting [DEFAULT] {key} (or legacy equivalent).")

    if not _cfg_get_with_legacy(config, KEY_SYSTEM_SCHEMA, LEGACY_KEY_SYSTEM_SCHEMA):
        errors.append(
            f"Missing required setting [DEFAULT] {KEY_SYSTEM_SCHEMA} "
            f"(legacy alias: {LEGACY_KEY_SYSTEM_SCHEMA})."
        )

    if not cfg_get(config, "temp_log_location") and not cfg_get(config, "temp", section="DMC_CONFIG"):
        errors.append(
            "Missing temp path: set [DEFAULT] temp_log_location or [DMC_CONFIG] temp."
        )

    if not config.has_section("FILES"):
        errors.append("Missing [FILES] section.")
    else:
        for key in _REQUIRED_FILES:
            if key not in config["FILES"]:
                errors.append(f"Missing [FILES] {key}.")

    if not config.has_section("LINEAGE_DIAGRAM"):
        errors.append("Missing [LINEAGE_DIAGRAM] section.")
    else:
        for key in _REQUIRED_LINEAGE:
            if key not in config["LINEAGE_DIAGRAM"]:
                errors.append(f"Missing [LINEAGE_DIAGRAM] {key}.")

    _ = runtime_mode(config)
    errors.extend(_validate_global_vacuum_hours(config))

    if errors:
        raise ConfigError(
            message="config.ini failed validation",
            error_code="HF020",
            details={"errors": errors},
        )

    if check_paths_exist:
        file_hunt = cfg_get(config, "file_hunt_path")
        if file_hunt and not os.path.isdir(file_hunt):
            raise ConfigError(
                message=f"file_hunt_path does not exist: {file_hunt}",
                error_code="HF021",
                details={"hint": "Create the directory or fix config.ini"},
            )
        master_name = cfg_get(config, "master_spec_name", section="FILES")
        master_path = os.path.join(file_hunt, master_name)
        if master_name and file_hunt and not os.path.isfile(master_path):
            raise ConfigError(
                message=f"Master specs file not found: {master_path}",
                error_code="HF021",
                details={"hint": "Place master_specs.xlsx under file_hunt_path"},
            )


def _validate_global_vacuum_hours(config: configparser.ConfigParser) -> list[str]:
    """Validate [DEFAULT] global_vacuum_hours when present."""
    raw = _cfg_get_with_legacy(
        config, KEY_GLOBAL_VACUUM_HOURS, LEGACY_KEY_GLOBAL_VACUUM_HOURS
    )
    if not raw:
        return []
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return [
            f"[DEFAULT] {KEY_GLOBAL_VACUUM_HOURS} must be an integer "
            f"(default {GLOBAL_VACUUM_HOURS_DEFAULT})."
        ]
    if value < GLOBAL_VACUUM_HOURS_MIN or value > GLOBAL_VACUUM_HOURS_MAX:
        return [
            f"[DEFAULT] {KEY_GLOBAL_VACUUM_HOURS} must be between "
            f"{GLOBAL_VACUUM_HOURS_MIN} and {GLOBAL_VACUUM_HOURS_MAX} inclusive "
            f"(got {value})."
        ]
    return []
