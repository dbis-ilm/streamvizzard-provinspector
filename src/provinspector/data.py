from abc import ABC
from dataclasses import dataclass, field
from typing import Any

from provinspector.domain.constants import OperatorStepType, PipelineChangeType


@dataclass
class PipelineChangeData(ABC):
    """
    Abstract base class for pipeline change data.

    Attributes:
        `id`
        `change_type`
    """

    id: str
    change_type: PipelineChangeType = field(init=False)


@dataclass
class OperatorCreationPipelineChangeData(PipelineChangeData):
    """
    Operation creation pipeline change data.

    Attributes:
        `id`: inherited from PipelineChangeData
        `operator_id`
        `operator_name`
        `operator_data`
    """

    operator_id: int
    operator_name: str
    operator_data: dict[str, Any]

    def __post_init__(self):
        self.change_type = PipelineChangeType.OPERATOR_CREATION


@dataclass
class OperatorModificationPipelineChangeData(PipelineChangeData):
    """
    Operation modification pipeline change data.

    Attributes:
        `id`: inherited from PipelineChangeData
        `operator_id`
        `changed_parameter`
        `changed_value`
    """

    operator_id: int
    operator_name: str
    changed_parameter: str
    changed_value: float

    def __post_init__(self):
        self.change_type = PipelineChangeType.OPERATOR_MODIFICATION


@dataclass
class OperatorDeletionPipelineChangeData(PipelineChangeData):
    """
    Operation deletion pipeline change data.

    Attributes:
        `id`: inherited from PipelineChangeData
        `operator_id`
    """

    operator_id: int
    operator_name: str

    def __post_init__(self):
        self.change_type = PipelineChangeType.OPERATOR_DELETION


@dataclass
class ConnectionCreationPipelineChangeData(PipelineChangeData):
    """
    Connection creation pipeline change data.

    Attributes:
        `id`: inherited from PipelineChangeData
        `connection_id`
        `from_operator_id`
        `to_operator_id`
        `from_socket_id`
        `to_socket_id`
    """

    connection_id: int
    from_operator_id: int
    to_operator_id: int
    from_socket_id: int
    to_socket_id: int

    def __post_init__(self):
        self.change_type = PipelineChangeType.CONNECTION_CREATION


@dataclass
class ConnectionDeletionPipelineChangeData(PipelineChangeData):
    """
    Connection deletion pipeline change data.

    Attributes:
        `id`: inherited from PipelineChangeData
        `connection_id`
    """

    connection_id: int
    from_operator_id: int
    to_operator_id: int
    from_socket_id: int
    to_socket_id: int

    def __post_init__(self):
        self.change_type = PipelineChangeType.CONNECTION_DELETION


@dataclass
class MetricData:
    """
    Metric data.

    Attributes:
        `name`
        `value`
    """

    name: str
    value: float


@dataclass
class DebugStepData:
    """
    Debug step data relevant for the provenance inspector.

    Attributes:
        `id`
        `timestamp`
        `branch_id`
        `branch_local_step_id`
        `parent_branch_id`
        `operator_id`
        `operator_name`
        `operator_step_type`
        `operator_metrics`
        `changes`
    """

    id: str
    timestamp: float
    branch_id: int
    branch_local_step_id: int
    parent_branch_id: int | None
    operator_id: int
    operator_name: str
    operator_step_type: OperatorStepType
    operator_metrics: list[MetricData] | None
    changes: list[PipelineChangeData] | None
