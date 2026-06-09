"""Exhaustive E2E test case matrix with parent/child IDs and human-readable names."""

from __future__ import annotations

from dataclasses import dataclass, field

from tests.e2e.dq_profiles import LOAD_TYPE_LABELS
from tests.e2e.system_ops import RESTORE_SCENARIOS, VACUUM_HOURS_VALUES

LOAD_TYPES = ["FULL_LOAD", "APPEND_LOAD", "INCREMENTAL_CDC", "SCD_TYPE_2"]
SCALES_QUICK = [100, 1_000, 10_000]
SCALES_FULL = [100, 1_000, 10_000, 100_000]
SCALES_HEAVY = [100, 1_000, 10_000, 100_000, 500_000, 1_000_000]
SCALES_EXTREME = [1_000_000, 2_000_000, 5_000_000, 10_000_000]
MULTI_MILLION_CHANGE_SCALES = [1_000_000, 5_000_000]

CHANGE_PRESETS = [
    ("Initial Load", 0, 0, 0),
    ("5% Insert / 10% Update / 5% Delete", 5, 10, 5),
    ("20% Insert / 20% Update / 20% Delete", 20, 20, 20),
    ("50% Insert / 50% Update / 50% Delete", 50, 50, 50),
]
HEAVY_CHANGE_PRESETS = [
    ("10% Insert / 10% Update / 10% Delete", 10, 10, 10),
    ("30% Insert / 30% Update / 30% Delete", 30, 30, 30),
    ("5% Insert Only", 5, 0, 0),
    ("5% Update Only", 0, 5, 0),
    ("5% Delete Only", 0, 0, 5),
]
PARTITION_MATRIX = [
    ([], "No Partitioning"),
    (["country"], "Single Column (country)"),
    (["country", "status"], "Multi Column (country, status)"),
]
PARTITION_MIGRATIONS = [
    ([], ["country"], "Unpartitioned to Single Column"),
    (["country"], ["country", "status"], "Single to Multi Column"),
    (["country", "status"], [], "Multi Column to Unpartitioned"),
]
PARALLELISM_VALUES = [2, 4, 8, 16, 32]
DQ_PROFILES = [
    ("none", "No DQ Checks"),
    ("standard_pass", "Standard Checks Pass"),
    ("standard_fail", "Standard Checks Fail (Expect Block)"),
    ("pre_load_pass", "PRE_LOAD Comprehensive Pass"),
    ("pre_load_fail", "PRE_LOAD Comprehensive Fail (Expect Block)"),
    ("post_load_pass", "POST_LOAD Comprehensive Pass"),
    ("post_load_fail", "POST_LOAD Comprehensive Fail (Expect Report)"),
    ("full_dq", "Full DQ Pipeline (Standard + Pre + Post)"),
]
MULTI_CYCLE_ROUNDS = [
    [(5, 5, 5), (5, 5, 5), (5, 5, 5)],
    [(10, 10, 10), (10, 10, 10)],
    [(2, 2, 2), (2, 2, 2), (2, 2, 2), (2, 2, 2), (2, 2, 2)],
]


@dataclass
class TestCase:
    test_id: int
    parent_id: int
    test_name: str
    load_type: str
    row_count: int
    partition_keys: list[str] = field(default_factory=list)
    insert_pct: float = 0
    update_pct: float = 0
    delete_pct: float = 0
    change_rounds: list[tuple[float, float, float]] = field(default_factory=list)
    partition_migrate: list[list[str]] = field(default_factory=list)
    parallelism: int | None = None
    idempotency: bool = False
    expect_fail: bool = False
    expect_dq_block: bool = False
    expect_post_load_fail: bool = False
    configured_feed: bool = False
    dq_profile: str = "none"
    partition_remove: bool = False
    partition_add: bool = False
    category: str = ""
    inject_dq_fail_data: bool = False
    skip_validation: bool = False
    expect_skip: bool = False
    heavy_validation: bool = False
    dq_after_load: bool = False
    vacuum_cleanup: bool = False
    global_vacuum_hours: int | None = None
    inject_stale_rows: bool = False
    restore_point_count: int = 0
    restore_target_index: int = 0
    mutate_before_restore: bool = True


def _next_id(counter: list[int]) -> int:
    counter[0] += 1
    return counter[0]


def _scales_for_mode(mode: str) -> list[int]:
    if mode == "extreme":
        return SCALES_HEAVY
    if mode == "heavy":
        return SCALES_HEAVY
    if mode == "full":
        return SCALES_FULL
    return SCALES_QUICK


def _include_scale(scale: int, mode: str) -> bool:
    if mode == "quick":
        return scale <= 10_000
    if mode == "full":
        return scale <= 100_000
    if mode == "heavy":
        return scale <= 1_000_000
    return scale <= 1_000_000


def _include_change_at_scale(scale: int, mode: str) -> bool:
    if mode == "quick":
        return scale <= 1_000
    if mode == "full":
        return scale <= 10_000
    if mode == "heavy":
        return scale <= 1_000_000
    return scale <= 5_000_000


def _add_dq_case(
    cases: list[TestCase],
    cid: list[int],
    *,
    lt: str,
    profile: str,
    label: str,
    row_count: int,
    parent: int,
    category: str = "Data Quality",
    partition_keys: list[str] | None = None,
) -> int:
    expect_block = profile in ("standard_fail", "pre_load_fail")
    expect_post_fail = profile == "post_load_fail"
    tid = _next_id(cid)
    cases.append(
        TestCase(
            test_id=tid,
            parent_id=parent,
            test_name=f"{LOAD_TYPE_LABELS[lt]} - DQ: {label} ({row_count:,} rows)",
            load_type=lt,
            row_count=row_count,
            partition_keys=partition_keys or [],
            dq_profile=profile,
            expect_dq_block=expect_block,
            expect_fail=expect_block,
            expect_post_load_fail=expect_post_fail,
            inject_dq_fail_data=expect_block
            and profile in ("standard_fail", "pre_load_fail"),
            skip_validation=expect_block,
            heavy_validation=row_count >= 100_000,
            category=category,
        )
    )
    return tid


def _vacuum_hours_for_mode(mode: str) -> list[int]:
    if mode == "quick":
        return list(VACUUM_HOURS_VALUES)
    if mode == "full":
        return [168, 720, 8760]
    if mode in ("heavy", "extreme"):
        return list(VACUUM_HOURS_VALUES)
    return [168]


def _restore_scenarios_for_mode(mode: str) -> list[tuple[str, int, int]]:
    if mode == "quick":
        return [(name, *RESTORE_SCENARIOS[name]) for name in RESTORE_SCENARIOS]
    if mode == "full":
        return [(name, *RESTORE_SCENARIOS[name]) for name in RESTORE_SCENARIOS]
    if mode == "heavy":
        return [(name, *RESTORE_SCENARIOS[name]) for name in RESTORE_SCENARIOS]
    if mode == "extreme":
        return [
            (name, *RESTORE_SCENARIOS[name])
            for name in (
                "single",
                "double_first",
                "double_last",
                "triple_first",
                "triple_middle",
                "triple_last",
            )
        ]
    return [("single", *RESTORE_SCENARIOS["single"])]


def _system_ops_row_count(mode: str, *, with_changes: bool = False) -> int:
    if mode == "extreme":
        return 100_000 if with_changes else 50_000
    if mode in ("heavy", "full"):
        return 10_000 if with_changes else 5_000
    return 1_000


def _build_system_ops_cases(
    cid: list[int],
    lt_label: dict[str, str],
    initial_key_to_id: dict[tuple[str, int], int],
    *,
    mode: str,
    is_full_or_heavy: bool,
    is_heavy: bool,
    is_extreme: bool,
) -> list[TestCase]:
    """Vacuum-hour variants and multi-restore scenarios per load type."""
    out: list[TestCase] = []
    vacuum_hours = _vacuum_hours_for_mode(mode)
    restore_scenarios = _restore_scenarios_for_mode(mode)
    base_scale = _system_ops_row_count(mode)
    change_scale = _system_ops_row_count(mode, with_changes=True)

    sys_parent: dict[str, int] = {}

    for lt in LOAD_TYPES:
        parent = initial_key_to_id.get((lt, 100), 0)
        for hours in vacuum_hours:
            stale = mode in ("quick", "full", "heavy", "extreme")
            tid = _next_id(cid)
            if lt not in sys_parent:
                sys_parent[lt] = tid
            out.append(
                TestCase(
                    test_id=tid,
                    parent_id=parent,
                    test_name=(
                        f"{lt_label[lt]} - System Vacuum global_vacuum_hours={hours} "
                        f"({base_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=base_scale,
                    partition_keys=["country"] if base_scale >= 10_000 else [],
                    vacuum_cleanup=True,
                    global_vacuum_hours=hours,
                    inject_stale_rows=stale,
                    heavy_validation=base_scale >= 10_000,
                    category="System Vacuum",
                )
            )

        for scenario_name, rp_count, rp_target in restore_scenarios:
            tid = _next_id(cid)
            out.append(
                TestCase(
                    test_id=tid,
                    parent_id=sys_parent.get(lt, parent),
                    test_name=(
                        f"{lt_label[lt]} - System Restore: {scenario_name.replace('_', ' ').title()} "
                        f"({base_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=base_scale,
                    partition_keys=["country"] if base_scale >= 10_000 else [],
                    restore_point_count=rp_count,
                    restore_target_index=rp_target,
                    mutate_before_restore=True,
                    heavy_validation=base_scale >= 10_000,
                    category="System Restore",
                )
            )

        if is_full_or_heavy:
            tid = _next_id(cid)
            out.append(
                TestCase(
                    test_id=tid,
                    parent_id=sys_parent.get(lt, parent),
                    test_name=(
                        f"{lt_label[lt]} - System Restore After 20% Changes "
                        f"({change_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=change_scale,
                    partition_keys=["country"],
                    insert_pct=20,
                    update_pct=20,
                    delete_pct=20,
                    restore_point_count=2,
                    restore_target_index=0,
                    mutate_before_restore=True,
                    heavy_validation=True,
                    category="System Restore",
                )
            )
            out.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=tid,
                    test_name=(
                        f"{lt_label[lt]} - Vacuum {VACUUM_HOURS_VALUES[1]}h Then Triple Restore "
                        f"({change_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=change_scale,
                    partition_keys=["country"],
                    insert_pct=10,
                    update_pct=10,
                    delete_pct=10,
                    vacuum_cleanup=True,
                    global_vacuum_hours=720,
                    inject_stale_rows=True,
                    restore_point_count=3,
                    restore_target_index=1,
                    mutate_before_restore=True,
                    heavy_validation=True,
                    category="System Operations",
                )
            )

        if mode == "quick":
            out.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=sys_parent.get(lt, parent),
                    test_name=(
                        f"{lt_label[lt]} - Vacuum 720h Then Triple Restore "
                        f"({base_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=base_scale,
                    partition_keys=["country"] if base_scale >= 1_000 else [],
                    insert_pct=10,
                    update_pct=10,
                    delete_pct=10,
                    vacuum_cleanup=True,
                    global_vacuum_hours=720,
                    inject_stale_rows=True,
                    restore_point_count=3,
                    restore_target_index=1,
                    mutate_before_restore=True,
                    heavy_validation=False,
                    category="System Operations",
                )
            )

        if is_heavy or is_extreme:
            out.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=sys_parent.get(lt, parent),
                    test_name=(
                        f"{lt_label[lt]} - System Restore After Multi-Cycle Churn "
                        f"({change_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=change_scale,
                    partition_keys=["country"],
                    change_rounds=[(5, 5, 5), (5, 5, 5)],
                    restore_point_count=2,
                    restore_target_index=1,
                    mutate_before_restore=True,
                    heavy_validation=True,
                    category="System Restore",
                )
            )

        if is_extreme:
            mm_scale = 100_000
            out.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=sys_parent.get(lt, parent),
                    test_name=(
                        f"{lt_label[lt]} - Enterprise Vacuum 8760h + Triple Restore "
                        f"({mm_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=mm_scale,
                    partition_keys=["country"],
                    insert_pct=10,
                    update_pct=10,
                    delete_pct=10,
                    vacuum_cleanup=True,
                    global_vacuum_hours=8760,
                    inject_stale_rows=False,
                    restore_point_count=3,
                    restore_target_index=0,
                    mutate_before_restore=True,
                    heavy_validation=True,
                    category="System Operations",
                )
            )

    return out


def build_test_matrix(
    *,
    mode: str = "quick",
    include_configured_feed: bool = True,
) -> list[TestCase]:
    """
    Build test cases.

    mode: smoke | quick | full | heavy | extreme
    """
    cases: list[TestCase] = []
    cid = [0]
    lt_label = LOAD_TYPE_LABELS
    scales = _scales_for_mode(mode)
    is_extreme = mode == "extreme"
    is_heavy = mode in ("heavy", "extreme")
    is_full_or_heavy = mode in ("full", "heavy", "extreme")

    if mode == "smoke":
        for lt in LOAD_TYPES:
            cases.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=0,
                    test_name=(
                        f"{lt_label[lt]} - Initial Load + Vacuum global_vacuum_hours=168 "
                        "+ Single Restore (100 rows)"
                    ),
                    load_type=lt,
                    row_count=100,
                    vacuum_cleanup=True,
                    global_vacuum_hours=168,
                    restore_point_count=1,
                    mutate_before_restore=True,
                    category="Smoke",
                )
            )
        return cases

    # --- 1. Load type correctness (all scales) ---
    initial_key_to_id: dict[tuple[str, int], int] = {}
    for lt in LOAD_TYPES:
        for scale in scales:
            if not _include_scale(scale, mode):
                continue
            for preset_label, ins, upd, del_ in CHANGE_PRESETS:
                if preset_label != "Initial Load" and not _include_change_at_scale(
                    scale, mode
                ):
                    continue
                parent = 0
                if preset_label != "Initial Load":
                    parent = initial_key_to_id.get((lt, scale), 0)
                tid = _next_id(cid)
                if preset_label == "Initial Load":
                    initial_key_to_id[(lt, scale)] = tid
                name = (
                    f"{lt_label[lt]} - {preset_label} ({scale:,} rows)"
                    if preset_label == "Initial Load"
                    else f"{lt_label[lt]} - After Changes: {preset_label} ({scale:,} rows)"
                )
                cases.append(
                    TestCase(
                        test_id=tid,
                        parent_id=parent,
                        test_name=name,
                        load_type=lt,
                        row_count=scale,
                        insert_pct=ins,
                        update_pct=upd,
                        delete_pct=del_,
                        heavy_validation=scale >= 100_000,
                        category="Load Correctness",
                    )
                )

    # --- 1b. Heavy-only granular I/U/D isolation ---
    if is_heavy:
        for lt in LOAD_TYPES:
            for preset_label, ins, upd, del_ in HEAVY_CHANGE_PRESETS:
                parent = initial_key_to_id.get((lt, 10_000), 0)
                cases.append(
                    TestCase(
                        test_id=_next_id(cid),
                        parent_id=parent,
                        test_name=(
                            f"{lt_label[lt]} - Isolated Change: {preset_label} "
                            "(10,000 rows)"
                        ),
                        load_type=lt,
                        row_count=10_000,
                        insert_pct=ins,
                        update_pct=upd,
                        delete_pct=del_,
                        heavy_validation=True,
                        category="Load Correctness",
                    )
                )

    # --- 2. Idempotency ---
    for lt in LOAD_TYPES:
        parent = initial_key_to_id.get((lt, 100), 0)
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=parent,
                test_name=f"{lt_label[lt]} - Idempotency Double Run (100 rows)",
                load_type=lt,
                row_count=100,
                idempotency=True,
                category="Idempotency",
            )
        )
    if is_full_or_heavy:
        for lt in LOAD_TYPES:
            parent = initial_key_to_id.get((lt, 10_000), 0)
            cases.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=parent,
                    test_name=f"{lt_label[lt]} - Idempotency Double Run (10,000 rows)",
                    load_type=lt,
                    row_count=10_000,
                    idempotency=True,
                    heavy_validation=True,
                    category="Idempotency",
                )
            )

    # --- 3. Partitioning ---
    part_parent: dict[str, int] = {}
    part_scale = 10_000 if is_heavy else 1_000
    for lt in LOAD_TYPES:
        for keys, label in PARTITION_MATRIX:
            tid = _next_id(cid)
            if not keys:
                part_parent[lt] = tid
            cases.append(
                TestCase(
                    test_id=tid,
                    parent_id=part_parent.get(lt, 0) if keys else 0,
                    test_name=(
                        f"{lt_label[lt]} - Partitioning: {label} "
                        f"({part_scale:,} rows)"
                    ),
                    load_type=lt,
                    row_count=part_scale,
                    partition_keys=keys,
                    heavy_validation=part_scale >= 10_000,
                    category="Partitioning",
                )
            )

    # --- 4. Partition remove / add ---
    for lt in LOAD_TYPES:
        parent = part_parent.get(lt, 0)
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=parent,
                test_name=(
                    f"{lt_label[lt]} - Remove Partitioning After Partitioned Load "
                    f"({part_scale:,} rows)"
                ),
                load_type=lt,
                row_count=part_scale,
                partition_keys=[],
                partition_remove=True,
                heavy_validation=part_scale >= 10_000,
                category="Partitioning",
            )
        )
        tid = _next_id(cid)
        cases.append(
            TestCase(
                test_id=tid,
                parent_id=part_parent.get(lt, 0),
                test_name=(
                    f"{lt_label[lt]} - Add Partitioning After Unpartitioned Load "
                    f"({part_scale:,} rows)"
                ),
                load_type=lt,
                row_count=part_scale,
                partition_keys=["country"],
                partition_add=True,
                heavy_validation=part_scale >= 10_000,
                category="Partitioning",
            )
        )

    # --- 4b. Partition migration chains (full / heavy) ---
    if is_full_or_heavy:
        for lt in LOAD_TYPES:
            for start_keys, end_keys, mig_label in PARTITION_MIGRATIONS:
                tid = _next_id(cid)
                cases.append(
                    TestCase(
                        test_id=tid,
                        parent_id=part_parent.get(lt, 0),
                        test_name=(
                            f"{lt_label[lt]} - Partition Migration: {mig_label} "
                            "(10,000 rows)"
                        ),
                        load_type=lt,
                        row_count=10_000,
                        partition_keys=start_keys,
                        partition_migrate=[end_keys],
                        heavy_validation=True,
                        category="Partitioning",
                    )
                )

    # --- 5. Parallelism (Full Load) ---
    par_parent = 0
    for i, par in enumerate(PARALLELISM_VALUES):
        tid = _next_id(cid)
        if i == 0:
            par_parent = tid
        cases.append(
            TestCase(
                test_id=tid,
                parent_id=0 if i == 0 else par_parent,
                test_name=f"Full Load - Spark Parallelism {par} (10,000 rows)",
                load_type="FULL_LOAD",
                row_count=10_000 if is_full_or_heavy else 1_000,
                parallelism=par,
                heavy_validation=is_full_or_heavy,
                category="Parallelism",
            )
        )

    # --- 6. Data quality (all load types × all DQ profiles) ---
    dq_scales = [500]
    if is_full_or_heavy:
        dq_scales.extend([10_000])
    if is_heavy:
        dq_scales.append(100_000)

    for lt in LOAD_TYPES:
        lt_dq_parent = 0
        first_dq = True
        for profile, label in DQ_PROFILES:
            if profile == "none":
                continue
            for dq_scale in dq_scales:
                tid = _next_id(cid)
                if first_dq:
                    lt_dq_parent = tid
                    parent = 0
                    first_dq = False
                else:
                    parent = lt_dq_parent
                expect_block = profile in ("standard_fail", "pre_load_fail")
                expect_post_fail = profile == "post_load_fail"
                part_keys = ["country"] if dq_scale >= 10_000 else []
                cases.append(
                    TestCase(
                        test_id=tid,
                        parent_id=parent,
                        test_name=(
                            f"{lt_label[lt]} - DQ: {label} ({dq_scale:,} rows)"
                        ),
                        load_type=lt,
                        row_count=dq_scale,
                        partition_keys=part_keys,
                        dq_profile=profile,
                        expect_dq_block=expect_block,
                        expect_fail=expect_block,
                        expect_post_load_fail=expect_post_fail,
                        inject_dq_fail_data=expect_block
                        and profile in ("standard_fail", "pre_load_fail"),
                        skip_validation=expect_block,
                        heavy_validation=dq_scale >= 100_000,
                        dq_after_load=profile in ("post_load_pass", "post_load_fail", "full_dq"),
                        category="Data Quality",
                    )
                )

    # --- 6b. DQ + changes at scale (heavy) ---
    if is_heavy:
        for lt in LOAD_TYPES:
            parent = 0
            for profile in ("standard_pass", "pre_load_pass", "post_load_pass", "full_dq"):
                label = next(l for p, l in DQ_PROFILES if p == profile)
                tid = _add_dq_case(
                    cases,
                    cid,
                    lt=lt,
                    profile=profile,
                    label=f"{label} Then 20% Source Changes",
                    row_count=50_000,
                    parent=parent,
                    category="Data Quality",
                    partition_keys=["country"],
                )
                if parent == 0:
                    parent = tid
                idx = len(cases) - 1
                cases[idx].insert_pct = 20
                cases[idx].update_pct = 20
                cases[idx].delete_pct = 20
                cases[idx].dq_after_load = True
                cases[idx].heavy_validation = True
                cases[idx].skip_validation = False

    # --- 7. Incremental CDC heavy suite ---
    cdc_parent = initial_key_to_id.get(("INCREMENTAL_CDC", 10_000), 0)
    cases.append(
        TestCase(
            test_id=_next_id(cid),
            parent_id=cdc_parent,
            test_name="Incremental CDC - 50% Churn Single Pass (100,000 rows)",
            load_type="INCREMENTAL_CDC",
            row_count=100_000,
            insert_pct=50,
            update_pct=50,
            delete_pct=50,
            heavy_validation=True,
            category="Incremental CDC",
        )
    )
    if is_heavy:
        for i, rounds in enumerate(MULTI_CYCLE_ROUNDS):
            cases.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=cdc_parent,
                    test_name=(
                        f"Incremental CDC - Multi-Cycle Churn "
                        f"({len(rounds)} rounds, 10,000 rows)"
                    ),
                    load_type="INCREMENTAL_CDC",
                    row_count=10_000,
                    change_rounds=rounds,
                    partition_keys=["country"],
                    heavy_validation=True,
                    category="Incremental CDC",
                )
            )
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=cdc_parent,
                test_name="Incremental CDC - Partitioned 500k + 20% Changes",
                load_type="INCREMENTAL_CDC",
                row_count=500_000,
                partition_keys=["country"],
                insert_pct=20,
                update_pct=20,
                delete_pct=20,
                heavy_validation=True,
                category="Incremental CDC",
            )
        )

    # --- 8. Append Load heavy suite ---
    append_parent = initial_key_to_id.get(("APPEND_LOAD", 10_000), 0)
    cases.append(
        TestCase(
            test_id=_next_id(cid),
            parent_id=append_parent,
            test_name="Append Load - 20% Inserts Only (10,000 rows)",
            load_type="APPEND_LOAD",
            row_count=10_000,
            insert_pct=20,
            update_pct=0,
            delete_pct=0,
            heavy_validation=True,
            category="Append Load",
        )
    )
    cases.append(
        TestCase(
            test_id=_next_id(cid),
            parent_id=append_parent,
            test_name="Append Load - Updates/Deletes Ignored (10,000 rows)",
            load_type="APPEND_LOAD",
            row_count=10_000,
            insert_pct=0,
            update_pct=20,
            delete_pct=20,
            heavy_validation=True,
            category="Append Load",
        )
    )
    if is_heavy:
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=append_parent,
                test_name="Append Load - Five Consecutive 5% Insert Rounds (50,000 rows)",
                load_type="APPEND_LOAD",
                row_count=50_000,
                change_rounds=[(5, 0, 0)] * 5,
                partition_keys=["country"],
                heavy_validation=True,
                category="Append Load",
            )
        )
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=append_parent,
                test_name="Append Load - Partitioned 500k Initial + 10% Inserts",
                load_type="APPEND_LOAD",
                row_count=500_000,
                partition_keys=["country", "status"],
                insert_pct=10,
                update_pct=0,
                delete_pct=0,
                heavy_validation=True,
                category="Append Load",
            )
        )

    # --- 9. SCD Type 2 heavy suite ---
    scd_parent = initial_key_to_id.get(("SCD_TYPE_2", 10_000), 0)
    cases.append(
        TestCase(
            test_id=_next_id(cid),
            parent_id=scd_parent,
            test_name="SCD Type 2 - 50% Attribute Changes (10,000 rows)",
            load_type="SCD_TYPE_2",
            row_count=10_000,
            insert_pct=10,
            update_pct=50,
            delete_pct=10,
            heavy_validation=True,
            category="SCD Type 2",
        )
    )
    if is_heavy:
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=scd_parent,
                test_name="SCD Type 2 - Version History Multi-Cycle (10,000 rows)",
                load_type="SCD_TYPE_2",
                row_count=10_000,
                change_rounds=[(0, 10, 0), (0, 10, 0), (0, 10, 0)],
                partition_keys=["country"],
                heavy_validation=True,
                category="SCD Type 2",
            )
        )
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=scd_parent,
                test_name="SCD Type 2 - Partition Migration + 20% Updates (10,000 rows)",
                load_type="SCD_TYPE_2",
                row_count=10_000,
                partition_keys=[],
                partition_migrate=[["country"]],
                insert_pct=0,
                update_pct=20,
                delete_pct=0,
                heavy_validation=True,
                category="SCD Type 2",
            )
        )
        cases.append(
            TestCase(
                test_id=_next_id(cid),
                parent_id=scd_parent,
                test_name="SCD Type 2 - Partitioned 100k + 20% Changes",
                load_type="SCD_TYPE_2",
                row_count=100_000,
                partition_keys=["country"],
                insert_pct=20,
                update_pct=20,
                delete_pct=20,
                heavy_validation=True,
                category="SCD Type 2",
            )
        )

    # --- 10. Real-world pipeline scenarios (heavy) ---
    if is_heavy:
        for lt in LOAD_TYPES:
            tid = _next_id(cid)
            cases.append(
                TestCase(
                    test_id=tid,
                    parent_id=0,
                    test_name=(
                        f"{lt_label[lt]} - Petabyte Proxy Pipeline: "
                        "1M Initial + DQ + 10% Churn + Partition Migration"
                    ),
                    load_type=lt,
                    row_count=1_000_000,
                    partition_keys=["country"],
                    dq_profile="full_dq",
                    dq_after_load=True,
                    change_rounds=[(10, 10, 10)],
                    partition_migrate=(
                        [["country", "status"]]
                        if lt == "APPEND_LOAD"
                        else [["country", "status"], []]
                    ),
                    heavy_validation=True,
                    category="Real World Pipeline",
                )
            )

    # --- 11. Large-scale performance ---
    if is_full_or_heavy:
        perf_parent = 0
        large_scales = [100_000]
        if is_heavy:
            large_scales.extend([500_000, 1_000_000])
        for scale in large_scales:
            for i, lt in enumerate(LOAD_TYPES):
                tid = _next_id(cid)
                if i == 0 and scale == large_scales[0]:
                    perf_parent = tid
                cases.append(
                    TestCase(
                        test_id=tid,
                        parent_id=0 if i == 0 and scale == large_scales[0] else perf_parent,
                        test_name=(
                            f"{lt_label[lt]} - Large Scale Initial Load "
                            f"({scale:,} rows)"
                        ),
                        load_type=lt,
                        row_count=scale,
                        partition_keys=["country"] if scale >= 100_000 else [],
                        heavy_validation=True,
                        category="Large Scale",
                    )
                )
                cases.append(
                    TestCase(
                        test_id=_next_id(cid),
                        parent_id=tid,
                        test_name=(
                            f"{lt_label[lt]} - Large Scale After 20% Changes "
                            f"({scale:,} rows)"
                        ),
                        load_type=lt,
                        row_count=scale,
                        partition_keys=["country"] if scale >= 100_000 else [],
                        insert_pct=20,
                        update_pct=20,
                        delete_pct=20,
                        heavy_validation=True,
                        category="Large Scale",
                    )
                )

    # --- 12. High-churn at 10k (full / heavy) ---
    if is_full_or_heavy:
        for lt in LOAD_TYPES:
            parent = initial_key_to_id.get((lt, 10_000), 0)
            cases.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=parent,
                    test_name=(
                        f"{lt_label[lt]} - After Changes: "
                        "20% Insert / 20% Update / 20% Delete (10,000 rows)"
                    ),
                    load_type=lt,
                    row_count=10_000,
                    insert_pct=20,
                    update_pct=20,
                    delete_pct=20,
                    heavy_validation=True,
                    category="Load Correctness",
                )
            )

    # --- 13. Error handling ---
    cases.extend(
        [
            TestCase(
                test_id=_next_id(cid),
                parent_id=0,
                test_name="Error Handling - Empty Source Table",
                load_type="FULL_LOAD",
                row_count=0,
                expect_fail=True,
                skip_validation=True,
                category="Error Handling",
            ),
            TestCase(
                test_id=_next_id(cid),
                parent_id=0,
                test_name="Error Handling - Load Type Conflict on Target",
                load_type="APPEND_LOAD",
                row_count=100,
                expect_fail=True,
                skip_validation=True,
                category="Error Handling",
            ),
            TestCase(
                test_id=_next_id(cid),
                parent_id=0,
                test_name="Error Handling - Null Values in Partition Column",
                load_type="FULL_LOAD",
                row_count=100,
                partition_keys=["status"],
                expect_fail=True,
                skip_validation=True,
                category="Error Handling",
            ),
            TestCase(
                test_id=_next_id(cid),
                parent_id=0,
                test_name="Incremental CDC - No Source Changes Re-Run (Expect Skip)",
                load_type="INCREMENTAL_CDC",
                row_count=500,
                idempotency=True,
                expect_skip=True,
                skip_validation=True,
                category="Error Handling",
            ),
            TestCase(
                test_id=_next_id(cid),
                parent_id=0,
                test_name="Full Load - No Source Changes Re-Run (Expect Skip)",
                load_type="FULL_LOAD",
                row_count=500,
                idempotency=True,
                expect_skip=True,
                skip_validation=True,
                category="Error Handling",
            ),
            TestCase(
                test_id=_next_id(cid),
                parent_id=0,
                test_name="Append Load - No Source Changes Re-Run (Expect Skip)",
                load_type="APPEND_LOAD",
                row_count=500,
                idempotency=True,
                expect_skip=True,
                skip_validation=True,
                category="Error Handling",
            ),
            TestCase(
                test_id=_next_id(cid),
                parent_id=0,
                test_name="SCD Type 2 - No Source Changes Re-Run (Expect Skip)",
                load_type="SCD_TYPE_2",
                row_count=500,
                idempotency=True,
                expect_skip=True,
                skip_validation=True,
                category="Error Handling",
            ),
        ]
    )

    # --- 14. Multi-million enterprise suite (extreme) ---
    if is_extreme:
        mm_parent: dict[str, int] = {}
        for lt in LOAD_TYPES:
            for scale in SCALES_EXTREME:
                tid = _next_id(cid)
                if scale == SCALES_EXTREME[0]:
                    mm_parent[lt] = tid
                cases.append(
                    TestCase(
                        test_id=tid,
                        parent_id=0 if scale == SCALES_EXTREME[0] else mm_parent[lt],
                        test_name=(
                            f"{lt_label[lt]} - Multi-Million Initial Load "
                            f"({scale:,} rows)"
                        ),
                        load_type=lt,
                        row_count=scale,
                        partition_keys=["country"],
                        heavy_validation=True,
                        category="Multi-Million",
                    )
                )
                cases.append(
                    TestCase(
                        test_id=_next_id(cid),
                        parent_id=tid,
                        test_name=(
                            f"{lt_label[lt]} - Multi-Million 10% Churn "
                            f"({scale:,} rows)"
                        ),
                        load_type=lt,
                        row_count=scale,
                        partition_keys=["country"],
                        insert_pct=10,
                        update_pct=10,
                        delete_pct=10,
                        heavy_validation=True,
                        category="Multi-Million",
                    )
                )

        for scale in MULTI_MILLION_CHANGE_SCALES:
            for lt in LOAD_TYPES:
                cases.append(
                    TestCase(
                        test_id=_next_id(cid),
                        parent_id=mm_parent.get(lt, 0),
                        test_name=(
                            f"{lt_label[lt]} - Multi-Million DQ Full Pipeline "
                            f"({scale:,} rows)"
                        ),
                        load_type=lt,
                        row_count=scale,
                        partition_keys=["country"],
                        dq_profile="full_dq",
                        dq_after_load=True,
                        heavy_validation=True,
                        category="Multi-Million",
                    )
                )

        for lt in LOAD_TYPES:
            cases.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=mm_parent.get(lt, 0),
                    test_name=(
                        f"{lt_label[lt]} - 24x7 Enterprise Pipeline: "
                        "5M + 5 Churn Cycles + Partition Migration + Full DQ"
                    ),
                    load_type=lt,
                    row_count=5_000_000,
                    partition_keys=["country"],
                    dq_profile="full_dq",
                    dq_after_load=True,
                    change_rounds=[(2, 2, 2)] * 5,
                    partition_migrate=(
                        [["country", "status"]]
                        if lt == "APPEND_LOAD"
                        else [["country", "status"], []]
                    ),
                    heavy_validation=True,
                    category="Enterprise 24x7",
                )
            )

        for profile, label in DQ_PROFILES:
            if profile == "none":
                continue
            for lt in LOAD_TYPES:
                expect_block = profile in ("standard_fail", "pre_load_fail")
                expect_post_fail = profile == "post_load_fail"
                cases.append(
                    TestCase(
                        test_id=_next_id(cid),
                        parent_id=mm_parent.get(lt, 0),
                        test_name=(
                            f"{lt_label[lt]} - Multi-Million DQ: {label} "
                            "(1,000,000 rows)"
                        ),
                        load_type=lt,
                        row_count=1_000_000,
                        partition_keys=["country"],
                        dq_profile=profile,
                        expect_dq_block=expect_block,
                        expect_fail=expect_block,
                        expect_post_load_fail=expect_post_fail,
                        inject_dq_fail_data=expect_block
                        and profile in ("standard_fail", "pre_load_fail"),
                        skip_validation=expect_block,
                        heavy_validation=True,
                        category="Multi-Million DQ",
                    )
                )

    # --- 15. System vacuum + restore (all load types) ---
    cases.extend(
        _build_system_ops_cases(
            cid,
            lt_label,
            initial_key_to_id,
            mode=mode,
            is_full_or_heavy=is_full_or_heavy,
            is_heavy=is_heavy,
            is_extreme=is_extreme,
        )
    )

    # --- 16. Production configured feed ---
    if include_configured_feed:
        from tests.e2e.discover_feeds import discover_configured_feeds

        for row in discover_configured_feeds():
            import json

            fs = json.loads(row["feed_specs"])
            cases.append(
                TestCase(
                    test_id=_next_id(cid),
                    parent_id=0,
                    test_name=(
                        f"Production Feed - {row['feed_name']} "
                        f"(ID {row['feed_id']}, {row['load_type']})"
                    ),
                    load_type=row["load_type"],
                    row_count=100,
                    partition_keys=fs.get("partition_keys", []),
                    configured_feed=True,
                    category="Production Feed",
                )
            )
    return cases
