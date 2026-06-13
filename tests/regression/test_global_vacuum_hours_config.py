"""Config validation for global_vacuum_hours and system_schema."""

import configparser

import pytest

from handuflow.config.config_paths import global_vacuum_hours
from handuflow.config.validate import validate_handuflow_config
from handuflow.exception import ConfigError


def _minimal_config(**extra_default: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read_dict(
        {
            "DEFAULT": {
                "file_hunt_path": "/tmp/handuflow",
                "outbound_directory_name": "handuflow_outbound",
                "log_directory_name": "handuflow_logs",
                "temp_log_location": "/tmp/handuflow/temp",
                "system_schema": "system_admin",
                **extra_default,
            },
            "FILES": {"master_spec_name": "master_specs.xlsx"},
            "LINEAGE_DIAGRAM": {
                "BOX_WIDTH": "4.4",
                "BOX_HEIGHT": "2.2",
                "X_GAP": "2.0",
                "Y_GAP": "2.5",
                "ROOT_GAP": "2.0",
            },
            "PLATFORM": {"runtime_mode": "local"},
            "DMC_CONFIG": {"temp": "/tmp/handuflow/dmc_temp"},
        }
    )
    return cfg


def test_global_vacuum_hours_defaults_to_168_when_missing():
    cfg = _minimal_config()
    validate_handuflow_config(cfg, check_paths_exist=False)
    assert global_vacuum_hours(cfg) == 168


def test_global_vacuum_hours_accepts_valid_range():
    cfg = _minimal_config(global_vacuum_hours="720")
    validate_handuflow_config(cfg, check_paths_exist=False)
    assert global_vacuum_hours(cfg) == 720


@pytest.mark.parametrize("value", ["167", "8761", "abc"])
def test_global_vacuum_hours_rejects_invalid_values(value: str):
    cfg = _minimal_config(global_vacuum_hours=value)
    with pytest.raises(ConfigError):
        validate_handuflow_config(cfg, check_paths_exist=False)


def test_legacy_uppercase_global_vacuum_hours_still_works():
    cfg = _minimal_config(GLOBAL_VACUUM_HOURS="720")
    validate_handuflow_config(cfg, check_paths_exist=False)
    assert global_vacuum_hours(cfg) == 720


def test_legacy_uppercase_system_schema_still_works():
    cfg = configparser.ConfigParser()
    cfg.read_dict(
        {
            "DEFAULT": {
                "file_hunt_path": "/tmp/handuflow",
                "outbound_directory_name": "handuflow_outbound",
                "log_directory_name": "handuflow_logs",
                "temp_log_location": "/tmp/handuflow/temp",
                "SYSTEM_SCHEMA": "legacy_admin",
            },
            "FILES": {"master_spec_name": "master_specs.xlsx"},
            "LINEAGE_DIAGRAM": {
                "BOX_WIDTH": "4.4",
                "BOX_HEIGHT": "2.2",
                "X_GAP": "2.0",
                "Y_GAP": "2.5",
                "ROOT_GAP": "2.0",
            },
            "PLATFORM": {"runtime_mode": "local"},
            "DMC_CONFIG": {"temp": "/tmp/handuflow/dmc_temp"},
        }
    )
    validate_handuflow_config(cfg, check_paths_exist=False)
    from handuflow.config.config_paths import system_schema

    assert system_schema(cfg) == "legacy_admin"


def test_system_schema_is_mandatory():
    cfg = configparser.ConfigParser()
    cfg.read_dict(
        {
            "DEFAULT": {
                "file_hunt_path": "/tmp/handuflow",
                "outbound_directory_name": "handuflow_outbound",
                "log_directory_name": "handuflow_logs",
                "temp_log_location": "/tmp/handuflow/temp",
            },
            "FILES": {"master_spec_name": "master_specs.xlsx"},
            "LINEAGE_DIAGRAM": {
                "BOX_WIDTH": "4.4",
                "BOX_HEIGHT": "2.2",
                "X_GAP": "2.0",
                "Y_GAP": "2.5",
                "ROOT_GAP": "2.0",
            },
            "PLATFORM": {"runtime_mode": "local"},
            "DMC_CONFIG": {"temp": "/tmp/handuflow/dmc_temp"},
        }
    )
    with pytest.raises(ConfigError):
        validate_handuflow_config(cfg, check_paths_exist=False)


def test_global_vacuum_hours_boundary_values():
    for value in ("168", "8760"):
        cfg = _minimal_config(global_vacuum_hours=value)
        validate_handuflow_config(cfg, check_paths_exist=False)
        assert global_vacuum_hours(cfg) == int(value)


def test_is_auto_vacuum_enabled_defaults_true():
    from handuflow.config.config_paths import is_auto_vacuum_enabled

    cfg = _minimal_config()
    assert is_auto_vacuum_enabled(cfg) is True


@pytest.mark.parametrize("value,expected", [("true", True), ("false", False), ("0", False)])
def test_is_auto_vacuum_enabled_parses_bool(value: str, expected: bool):
    from handuflow.config.config_paths import is_auto_vacuum_enabled

    cfg = _minimal_config(is_auto_vacuum_enabled=value)
    assert is_auto_vacuum_enabled(cfg) is expected
