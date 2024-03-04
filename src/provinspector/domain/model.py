from __future__ import annotations

import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import prov.model

from provinspector.domain.constants import (
    OperatorStepType,
    PipelineChangeType,
    ProvType,
)
from provinspector.utils.prov_utils import document_factory, qualified_name


@dataclass
class PipelineVersion:
    """
    An entity representing a version of the pipeline.

    A `PipelineVersion` originates from a history split and respresents a history branch. A pipeline initially has one version (`PipelineVersion`) that represents the original execution branch.

    Attributes:
        `id_`
        `parent_pipeline_version_id`
        `created_at`
    """

    id_: int
    parent_pipeline_version_id: int | None

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        id_ = urllib.parse.quote_plus(
            str(self.id_), safe="", encoding=None, errors=None
        )

        return qualified_name(f"PipelineVersion?id={id_}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("id", self.id_),
            (prov.model.PROV_TYPE, ProvType.PIPELINE_VERSION),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class PipelineVersionRevision:
    """
    An entity representing a revision of a pipeline version.

    A `PipelineVersionRevision` originates from a change to a pipeline version (`PipelineVersion`), such as the creation or deletion of an operator, the modification of an operator's parameters, or the creation or deletion of a connection. A `PipelineVersion` initially has one revision (`PipelineVersionRevision`).

    A `PipelineVersionRevision` is a snapshot of the respective pipeline and comprises all corresponding `OperatorRevision`s and `Connection`s.

    Attributes:
        `uuid`
        `id_`
        `pipeline_version`
        `parent_pipeline_version_revision_uuid`
        `operators`
        `connections`
    """

    uuid: str
    id_: int
    pipeline_version: PipelineVersion
    parent_pipeline_version_revision_uuid: str | None
    operators: list[OperatorRevision]
    connections: list[Connection]

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        uuid = urllib.parse.quote_plus(self.uuid, safe="", encoding=None, errors=None)

        return qualified_name(f"PipelineVersionRevision?uuid={uuid}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("uuid", self.uuid),
            ("id", self.id_),
            (prov.model.PROV_TYPE, ProvType.PIPELINE_VERSION_REVISION),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class Operator:
    """
    An entity representing an operator of a pipeline.

    An `Operator` initially has one revision (`OperatorRevision`).

    Attributes:
        `id_`
        `name`
    """

    id_: int
    name: str

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        id_ = urllib.parse.quote_plus(
            str(self.id_), safe="", encoding=None, errors=None
        )

        return qualified_name(f"Operator?id={id_}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("id", self.id_),
            ("name", self.name),
            (prov.model.PROV_TYPE, ProvType.OPERATOR),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class OperatorRevision:
    """
    An entity representing a revision of an operator.

    Attributes:
        `uuid`
        `id_`
        `operator`
        `parameters`
        `parent_operator_revision`
    """

    uuid: str
    id_: int
    operator: Operator
    parameters: list[Parameter]
    parent_operator_revision_uuid: str | None

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        uuid = urllib.parse.quote_plus(self.uuid, safe="", encoding=None, errors=None)

        return qualified_name(f"OperatorRevision?uuid={uuid}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("uuid", self.uuid),
            ("id", self.id_),
            (prov.model.PROV_TYPE, ProvType.OPERATOR_REVISION),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class Parameter:
    """
    An entity representing a parameter of an operator.

    Attributes:
        `name`
        `value`
    """

    name: str
    value: Any

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        value_hash = hash(self.value)

        return qualified_name(f"Parameter?name={name}&value={value_hash}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", hash(self.value)),
            (prov.model.PROV_TYPE, ProvType.PARAMETER),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class OperatorRun:
    """
    An entity representing a collection of entities generated by the execution of an `OperatorRevision`.

    Attributes:
        `id_`
        `created_at`
        `metrics`: list of metrics created by the run
    """

    id_: str
    created_at: datetime
    metrics: list[Metric]

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        id_ = urllib.parse.quote_plus(self.id_, safe="", encoding=None, errors=None)

        return qualified_name(f"OperatorRun?id={id_}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("id", self.id_),
            ("time", self.created_at),
            (prov.model.PROV_TYPE, ProvType.OPERATOR_RUN),
            (prov.model.PROV_TYPE, ProvType.COLLECTION),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class Metric:
    """
    An entity representing a metric created by a run of an operator (more specifically, `OperatorRevision`).

    Attributes:
        `operator_revision_uuid`
        `name`
        `value`
        `created_at`
    """

    name: str
    value: float

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        name = urllib.parse.quote_plus(self.name, safe="", encoding=None, errors=None)
        value = str(self.value)

        return qualified_name(f"Metric?name={name}&value={value}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("name", self.name),
            ("value", self.value),
            (prov.model.PROV_TYPE, ProvType.METRIC),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class Connection:
    """
    An entity representing the connection between two operators.

    Attributes:
        `id_`
        `from_operator_id`
        `to_operator_id`
        `created_at`
    """

    id_: int
    from_operator_id: int
    to_operator_id: int

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        id_ = urllib.parse.quote_plus(
            str(self.id_), safe="", encoding=None, errors=None
        )

        return qualified_name(f"Connection?id={id_}")

    def to_prov(self) -> prov.model.ProvEntity:
        prov_attributes = [
            ("id", self.id_),
            ("from_operator_id", str(self.from_operator_id)),
            ("to_operator_id", str(self.to_operator_id)),
            (prov.model.PROV_TYPE, ProvType.CONNECTION),
        ]

        return prov.model.ProvEntity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class PipelineVersionCreation:
    """
    An activity representing the creation of a pipeline version.

    Attributes:
        `uuid`
        `pipeline_version_revision`
        `parent_pipeline_version_creation_uuid`
        `time`
    """

    uuid: str
    pipeline_version_revision: PipelineVersionRevision
    parent_pipeline_version_creation_uuid: str | None
    time: datetime

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        uuid = urllib.parse.quote_plus(
            str(self.uuid), safe="", encoding=None, errors=None
        )

        return qualified_name(f"PipelineVersionCreation?uuid={uuid}")

    def to_prov(self) -> prov.model.ProvActivity:
        prov_attributes = [
            ("uuid", self.uuid),
            (prov.model.PROV_ATTR_STARTTIME, self.time),
            (prov.model.PROV_ATTR_ENDTIME, self.time),
            (prov.model.PROV_TYPE, ProvType.PIPELINE_VERSION_CREATION),
        ]

        return prov.model.ProvActivity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class PipelineChange:
    """
    An activity representing different pipelines changes.

    A `PipelineChange` helps to track the changes between two `PipelineVersionRevision`s of a `PipelineVersion`, such as the creation or deletion of an operator, the modification of an operator's parameters, or the creation or deletion of a connection.

    Attributes:
        `uuid`
        `pipeline_change_type`
        `time`
        `operator_revision`
        `connection`
        `pipeline_version_revision`
        `parent_pipeline_change_uuid`
    """

    uuid: str
    pipeline_change_type: PipelineChangeType
    time: datetime
    operator_revision: OperatorRevision | None
    connection: Connection | None
    pipeline_version_revision: PipelineVersionRevision
    parent_pipeline_change_uuid: str | None

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        uuid = urllib.parse.quote_plus(
            str(self.uuid), safe="", encoding=None, errors=None
        )

        return qualified_name(f"PipelineChange?uuid={uuid}")

    def to_prov(self) -> prov.model.ProvActivity:
        prov_attributes = [
            ("uuid", self.uuid),
            ("pipeline_change_type", str(self.pipeline_change_type)),
            (prov.model.PROV_ATTR_STARTTIME, self.time),
            (prov.model.PROV_ATTR_ENDTIME, self.time),
            (prov.model.PROV_TYPE, ProvType.PIPELINE_CHANGE),
        ]

        return prov.model.ProvActivity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )


@dataclass
class OperatorCreationPipelineChange(PipelineChange):
    """
    An activity representing the pipeline change resulting from the creation of an operator.
    """

    pipeline_change_type: PipelineChangeType = field(init=False)
    operator_revision: OperatorRevision
    connection: Connection | None = field(init=False)

    def __post_init__(self):
        self.pipeline_change_type = PipelineChangeType.OPERATOR_CREATION
        self.connection = None


@dataclass
class OperatorModificationPipelineChange(PipelineChange):
    """
    An activity representing the pipeline change resulting from the modification of an operator.
    """

    pipeline_change_type: PipelineChangeType = field(init=False)
    operator_revision: OperatorRevision
    connection: Connection | None = field(init=False)

    def __post_init__(self):
        self.pipeline_change_type = PipelineChangeType.OPERATOR_MODIFICATION
        self.connection = None


@dataclass
class OperatorDeletionPipelineChange(PipelineChange):
    """
    An activity representing the pipeline change resulting from the deletion of an operator.
    """

    pipeline_change_type: PipelineChangeType = field(init=False)
    operator_revision: OperatorRevision
    connection: Connection | None = field(init=False)

    def __post_init__(self):
        self.pipeline_change_type = PipelineChangeType.OPERATOR_DELETION
        self.connection = None


@dataclass
class ConnectionCreationPipelineChange(PipelineChange):
    """
    An activity representing the pipeline change resulting from the creation of a connection.
    """

    pipeline_change_type: PipelineChangeType = field(init=False)
    connection: Connection
    operator_revision: OperatorRevision | None = field(init=False)

    def __post_init__(self):
        self.pipeline_change_type = PipelineChangeType.CONNECTION_CREATION
        self.operator_revision = None


@dataclass
class ConnectionDeletionPipelineChange(PipelineChange):
    """
    An activity representing the pipeline change resulting from the deletion of a connection.
    """

    pipeline_change_type: PipelineChangeType = field(init=False)
    connection: Connection
    operator_revision: OperatorRevision | None = field(init=False)

    def __post_init__(self):
        self.pipeline_change_type = PipelineChangeType.CONNECTION_DELETION
        self.operator_revision = None


@dataclass
class OperatorExecution:
    """
    An activity representing the execution of an operator (more specifically, `OperatorRevision`).

    Attributes:
        `uuid`
        `operator_revision`
        `operator_run`
        `operator_step_type`
        `time`
    """

    uuid: str
    operator_revision: OperatorRevision
    operator_run: OperatorRun
    operator_step_type: OperatorStepType
    time: datetime

    @property
    def prov_identifier(self) -> prov.model.QualifiedName:
        uuid = urllib.parse.quote_plus(
            str(self.uuid), safe="", encoding=None, errors=None
        )

        return qualified_name(f"OperatorExecution?uuid={uuid}")

    def to_prov(self) -> prov.model.ProvActivity:
        prov_attributes = [
            ("uuid", self.uuid),
            ("pipeline_change_type", str(self.operator_step_type)),
            (prov.model.PROV_ATTR_STARTTIME, self.time),
            (prov.model.PROV_ATTR_ENDTIME, self.time),
            (prov.model.PROV_TYPE, ProvType.OPERATOR_EXECUTION),
        ]

        return prov.model.ProvActivity(
            document_factory(),
            self.prov_identifier,
            prov_attributes,
        )
