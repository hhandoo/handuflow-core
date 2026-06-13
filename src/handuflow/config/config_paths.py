# inbuilt
import configparser

GLOBAL_VACUUM_HOURS_DEFAULT = 168
GLOBAL_VACUUM_HOURS_MIN = 168
GLOBAL_VACUUM_HOURS_MAX = 8760

# Canonical [DEFAULT] keys (PEP 8 / lowercase). Legacy uppercase keys remain supported.
KEY_SYSTEM_SCHEMA = "system_schema"
KEY_GLOBAL_VACUUM_HOURS = "global_vacuum_hours"
LEGACY_KEY_SYSTEM_SCHEMA = "SYSTEM_SCHEMA"
LEGACY_KEY_GLOBAL_VACUUM_HOURS = "GLOBAL_VACUUM_HOURS"


def _cfg_get_with_legacy(
    config: configparser.ConfigParser,
    key: str,
    legacy_key: str,
    default: str = "",
) -> str:
    """Read a config value preferring ``key``, then ``legacy_key``."""
    value = cfg_get(config, key, "").strip()
    if value:
        return value
    return cfg_get(config, legacy_key, default).strip()


def cfg_get(
    config: configparser.ConfigParser,
    key: str,
    default: str = "",
    *,
    section: str = "DEFAULT",
) -> str:
    """
    Read a config value from [DEFAULT] (parser defaults), explicit sections, or legacy sections.

    Note: configparser stores ``[DEFAULT]`` keys in :meth:`ConfigParser.defaults`, not as
    a normal section, so ``has_section('DEFAULT')`` is usually False.
    """
    if section == "DEFAULT":
        if key in config.defaults():
            return config.defaults()[key]
        if config.has_section("DEFAULT") and key in config["DEFAULT"]:
            return config["DEFAULT"][key]
    elif config.has_section(section) and key in config[section]:
        return config[section][key]

    if section != "DEFAULT":
        if key in config.defaults():
            return config.defaults()[key]

    for fallback_section in ("LOGGING", "DMC_CONFIG", "PLATFORM"):
        if config.has_section(fallback_section) and key in config[fallback_section]:
            return config[fallback_section][key]
    return default


def cfg_get_int(
    config: configparser.ConfigParser, key: str, default: int, *, section: str = "DEFAULT"
) -> int:
    raw = cfg_get(config, key, str(default), section=section)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def cfg_get_bool(
    config: configparser.ConfigParser,
    key: str,
    default: bool = False,
    *,
    section: str = "DEFAULT",
) -> bool:
    raw = cfg_get(config, key, str(default).lower(), section=section).strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def is_auto_vacuum_enabled(config: configparser.ConfigParser) -> bool:
    """When True, post-run cleanup runs Delta OPTIMIZE and VACUUM on spec tables."""
    return cfg_get_bool(config, "is_auto_vacuum_enabled", default=True)


def runtime_mode(config: configparser.ConfigParser) -> str:
    """Returns 'local' or 'unity_catalog'."""
    mode = cfg_get(config, "runtime_mode", "local", section="PLATFORM").lower()
    if mode in ("unity_catalog", "unity", "databricks", "uc"):
        return "unity_catalog"
    return "local"


def dmc_temp_dir(config: configparser.ConfigParser) -> str:
    return cfg_get(config, "temp", "", section="DMC_CONFIG") or cfg_get(
        config, "temp_directory", ""
    ) or cfg_get(config, "temp_log_location", "")


def system_schema(config: configparser.ConfigParser) -> str:
    """[DEFAULT] system_schema — schema for system metadata tables (mandatory at startup)."""
    return _cfg_get_with_legacy(config, KEY_SYSTEM_SCHEMA, LEGACY_KEY_SYSTEM_SCHEMA)


def global_vacuum_hours(config: configparser.ConfigParser) -> int:
    """
    Delta retention hours from [DEFAULT] global_vacuum_hours.

    Defaults to 168 when unset. Call :func:`validate_global_vacuum_hours` at startup.
    """
    raw = _cfg_get_with_legacy(config, KEY_GLOBAL_VACUUM_HOURS, LEGACY_KEY_GLOBAL_VACUUM_HOURS)
    if not raw:
        return GLOBAL_VACUUM_HOURS_DEFAULT
    try:
        return int(raw)
    except (TypeError, ValueError):
        return GLOBAL_VACUUM_HOURS_DEFAULT
