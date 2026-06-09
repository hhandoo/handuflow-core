from dataclasses import dataclass


@dataclass
class FeedDQSummaryRow:
    feed_name: str
    feed_ok: bool
    standard_checks_passed: bool
    pre_load_error: bool
    pre_load_warning_count: int
    executed_pre_load_checks: int