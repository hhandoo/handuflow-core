"""Unit tests for feed-level DQ gating (no Spark)."""

from handuflow.data_quality.runner.feed_data_quality_runner import FeedDataQualityRunner


def test_can_ingest_requires_standard_when_configured():
    assert (
        FeedDataQualityRunner._can_ingest(
            {
                "standard_checks_configured": True,
                "standard_checks_passed": False,
                "comprehensive_pre_load_configured": False,
            }
        )
        is False
    )


def test_can_ingest_requires_preload_comprehensive_when_configured():
    assert (
        FeedDataQualityRunner._can_ingest(
            {
                "standard_checks_configured": False,
                "comprehensive_pre_load_configured": True,
                "comprehensive_pre_load_passed": False,
            }
        )
        is False
    )
    assert (
        FeedDataQualityRunner.ingest_block_reason(
            {
                "comprehensive_pre_load_configured": True,
                "comprehensive_pre_load_passed": False,
            }
        )
        == "pre_load_comprehensive_checks_failed"
    )


def test_can_ingest_blocks_when_preload_configured_but_not_passed():
    assert (
        FeedDataQualityRunner._can_ingest(
            {
                "standard_checks_configured": False,
                "comprehensive_pre_load_configured": True,
                "comprehensive_pre_load_passed": None,
            }
        )
        is False
    )
    assert (
        FeedDataQualityRunner.ingest_block_reason(
            {
                "comprehensive_pre_load_configured": True,
                "comprehensive_pre_load_passed": None,
            }
        )
        == "pre_load_comprehensive_checks_not_passed"
    )


def test_can_ingest_blocks_when_standard_configured_but_not_passed():
    assert (
        FeedDataQualityRunner._can_ingest(
            {
                "standard_checks_configured": True,
                "standard_checks_passed": None,
                "comprehensive_pre_load_configured": False,
            }
        )
        is False
    )


def test_can_ingest_passes_when_no_checks_configured():
    assert FeedDataQualityRunner._can_ingest(
        {
            "standard_checks_configured": False,
            "standard_checks_passed": None,
            "comprehensive_pre_load_configured": False,
            "comprehensive_pre_load_passed": None,
        }
    )


def test_standard_passed_none_when_not_configured():
    """Dashboard must not show passed=TRUE when checks were not run."""
    result = {
        "standard_checks_configured": False,
        "standard_checks_passed": None,
        "comprehensive_pre_load_configured": False,
        "comprehensive_pre_load_passed": None,
    }
    assert result["standard_checks_passed"] is None
    assert FeedDataQualityRunner._can_ingest(result)


def test_comprehensive_checks_for_stage_filters():
    feed = {
        "comprehensive_checks": [
            {"check_name": "a", "load_stage": "PRE_LOAD"},
            {"check_name": "b", "load_stage": "POST_LOAD"},
            {"check_name": "c"},
        ]
    }
    pre = FeedDataQualityRunner._comprehensive_checks_for_stage(feed, "PRE_LOAD")
    post = FeedDataQualityRunner._comprehensive_checks_for_stage(feed, "POST_LOAD")
    assert len(pre) == 2
    assert len(post) == 1
    assert pre[1]["check_name"] == "c"
