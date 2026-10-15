"""Report types for the migrate orchestrator. Lives here (not in runner) to avoid cycles."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class ActionStatus(StrEnum):
    CREATED = "created"
    EXISTING = "existing"
    UPDATED = "updated"


@dataclass(frozen=True)
class MigrateAction:
    kind: str
    resource: str
    status: ActionStatus
    detail: str


@dataclass
class MigrateReport:
    actions: list[MigrateAction] = field(default_factory=list)

    def record(self, kind: str, resource: str, status: ActionStatus, detail: str) -> None:
        self.actions.append(MigrateAction(kind, resource, status, detail))

    def to_dict(self) -> dict[str, Any]:
        return {"actions": [{**asdict(a), "status": a.status.value} for a in self.actions]}
