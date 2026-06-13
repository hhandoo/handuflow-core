"""ResultGenerator sheet rules (no Spark)."""

import configparser

import pandas as pd
import pytest

from handuflow.data_movement_controller.data_class.load_result import LoadResult
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


def test_feed_status_sheet_uses_schema_route_label(minimal_config):
    cfg, root = minimal_config
    gen = ResultGenerator(
        payload=[
            {
                "feed_id": 1,
                "feed_name": "iso_language_codes",
                "target_schema_name": "silver",
                "source_table_name": "demo.test",
                "check_table_name": "demo.test",
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
    gen._ResultGenerator__generate_full_feed_status()
    sheet_names = [s["sheet_name"] for s in gen.sheets]
    assert "Feed Status (demo-silver)" in sheet_names
    assert "Feed Status (B-G)" not in sheet_names


def test_feed_status_includes_load_skipped(minimal_config):
    cfg, root = minimal_config
    gen = ResultGenerator(
        payload=[
            {
                "feed_id": 1,
                "feed_name": "iso_language_codes",
                "comprehensive_results": [],
                "standard_checks_result": [],
                "can_ingest": True,
            }
        ],
        file_hunt_path=root,
        run_id="test-run",
        config=cfg,
        system_report=pd.DataFrame(),
        load_results=[
            LoadResult(
                feed_id=1,
                success=True,
                skipped=True,
                total_rows_inserted=0,
                target_table_path="silver.t_iso_language_codes",
            )
        ],
    )
    gen._ResultGenerator__segregate_results()
    gen._ResultGenerator__merge_load_results_into_feed_status()
    row = gen.final_feed_status[0]
    assert row["load_skipped"] is True
    assert row["load_status"] == "SKIPPED"
    assert row["load_rows_inserted"] == 0


def test_feed_status_shows_blocked_when_dq_fails(minimal_config):
    cfg, root = minimal_config
    gen = ResultGenerator(
        payload=[
            {
                "feed_id": 1,
                "feed_name": "iso_language_codes",
                "comprehensive_results": [],
                "standard_checks_result": [],
                "can_ingest": False,
                "ingest_block_reason": "standard_checks_failed",
                "standard_checks_configured": True,
                "standard_checks_passed": False,
            }
        ],
        file_hunt_path=root,
        run_id="test-run",
        config=cfg,
        system_report=pd.DataFrame(),
        load_results=[],
    )
    gen._ResultGenerator__segregate_results()
    gen._ResultGenerator__merge_load_results_into_feed_status()
    gen._ResultGenerator__apply_dq_block_status()
    row = gen.final_feed_status[0]
    assert row["load_status"] == "BLOCKED"
    assert row["ingest_block_reason"] == "standard_checks_failed"
    assert row["load_success"] is False
