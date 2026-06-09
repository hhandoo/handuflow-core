"""ResultGenerator sheet rules (no Spark)."""

import configparser

import pandas as pd
import pytest

from handuflow.result_generator.result_generator import ResultGenerator


@pytest.fixture
def minimal_config(tmp_path):
    root = tmp_path / "dir"
    root.mkdir()
    cfg = configparser.ConfigParser()
    cfg.read_dict(
        {
            "DEFAULT": {
                "file_hunt_path": str(root),
                "outbound_directory_name": "outbound",
            },
        }
    )
    return cfg, str(root)


def test_no_comprehensive_sheet_when_checks_empty(minimal_config):
    cfg, root = minimal_config
    gen = ResultGenerator(
        payload=[
            {
                "feed_id": 3,
                "feed_name": "test",
                "comprehensive_results": [],
                "standard_checks_result": [],
                "can_ingest": True,
            }
        ],
        file_hunt_path=root,
        run_id="test-run",
        config=cfg,
        system_report=pd.DataFrame(),
        load_results=[],
    )
    gen._ResultGenerator__segregate_results()
    gen._ResultGenerator__generate_comprehensive_results()
    sheet_names = [s["sheet_name"] for s in gen.sheets]
    assert "Comprehensive Check Result" not in sheet_names


def test_comprehensive_sheet_only_with_real_checks(minimal_config):
    cfg, root = minimal_config
    gen = ResultGenerator(
        payload=[
            {
                "feed_id": 3,
                "feed_name": "test",
                "comprehensive_results": [
                    {
                        "check_name": "c1",
                        "load_stage": "PRE_LOAD",
                        "status": "PASSED",
                        "failed_records": 0,
                        "severity": "ERROR",
                    }
                ],
                "standard_checks_result": [],
                "can_ingest": True,
            }
        ],
        file_hunt_path=root,
        run_id="test-run",
        config=cfg,
        system_report=pd.DataFrame(),
        load_results=[],
    )
    gen._ResultGenerator__segregate_results()
    gen._ResultGenerator__generate_comprehensive_results()
    comp = [s for s in gen.sheets if s["sheet_name"] == "Comprehensive Check Result"]
    assert len(comp) == 1
    assert len(comp[0]["df"]) == 1
    assert comp[0]["df"].iloc[0]["check_name"] == "c1"
