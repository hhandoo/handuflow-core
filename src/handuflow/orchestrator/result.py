# inbuilt
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from handuflow.data_movement_controller.data_class.load_result import LoadResult

if TYPE_CHECKING:
    import pandas as pd


class RunStatus(str, Enum):
    """Terminal status for an :class:`~handuflow.orchestrator.orchestrator.Orchestrator` run."""

    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    FAILED = "FAILED"

    def __str__(self) -> str:
        return self.value


@dataclass
class RunResult:
    """
    Structured outcome from :meth:`handuflow.orchestrator.orchestrator.Orchestrator.run`.

    ``str(run_result)`` returns the status value for backward compatibility.
    """

    status: RunStatus
    run_id: str
    load_results: list[LoadResult] = field(default_factory=list)
    phase_errors: list[dict[str, Any]] = field(default_factory=list)
    archived_log_path: str | None = None
    message: str = ""
    master_specs: "pd.DataFrame | None" = None

    @property
    def succeeded(self) -> bool:
        return self.status in (RunStatus.COMPLETED, RunStatus.COMPLETED_WITH_ERRORS)

    def __str__(self) -> str:
        return self.status.value
