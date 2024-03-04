import abc
from dataclasses import dataclass, field
from typing import Any, Type

import prov.model

from provinspector.domain.constants import ProvRole
from provinspector.domain.model import (
    ConnectionCreationPipelineChange,
    ConnectionDeletionPipelineChange,
    OperatorCreationPipelineChange,
    OperatorDeletionPipelineChange,
    OperatorExecution,
    OperatorModificationPipelineChange,
    OperatorRevision,
    PipelineChange,
    PipelineVersionCreation,
    PipelineVersionRevision,
)

DEFAULT_NAMESPACE = prov.model.Namespace("ex", "example.org")


class ProvRevision(prov.model.ProvDerivation):
    """Provenance revision relationship."""

    # Override formal attributes and add revision attribute
    FORMAL_ATTRIBUTES = (
        prov.model.PROV_ATTR_GENERATED_ENTITY,
        prov.model.PROV_ATTR_USED_ENTITY,
        prov.model.PROV_ATTR_ACTIVITY,
        prov.model.PROV_ATTR_GENERATION,
        prov.model.PROV_ATTR_USAGE,
        prov.model.PROV["Revision"],
    )


@dataclass
class ProvContext:
    """
    The provenance context supporting the creation of provenance submodels.

    Attributes:
        `document`
        `namespace`
    """

    document: prov.model.ProvDocument
    namespace: str | None = None

    def add_element(
        self,
        dataclass_instance,
        check_exists=False,
    ) -> prov.model.ProvRecord:
        element = dataclass_instance.to_prov()

        if check_exists:
            record = self.document.get_record(element.identifier)
            if record:
                return record[0]

        return self.document.new_record(
            element._prov_type, element.identifier, element.attributes
        )

    def add_relation(
        self,
        source_dataclass_instance,
        target_dataclass_instance,
        relationship_type: Type[prov.model.ProvRelation],
        attributes: dict[str, Any] | None = None,
        check_exists=False,
    ) -> None:
        if not attributes:
            attributes = dict()

        source = source_dataclass_instance.to_prov()
        target = target_dataclass_instance.to_prov()

        relationship = self.document.new_record(
            record_type=relationship_type._prov_type,
            identifier=(
                prov.model.QualifiedName(
                    DEFAULT_NAMESPACE,
                    f"relation:{source.identifier}:{target.identifier}",
                )
                if relationship_type is not prov.model.ProvSpecialization
                and relationship_type is not prov.model.ProvMembership
                else None
            ),
            attributes={
                relationship_type.FORMAL_ATTRIBUTES[0]: source,  # type: ignore
                relationship_type.FORMAL_ATTRIBUTES[1]: target,  # type: ignore
            },
        )

        relationship.add_attributes(attributes)

        if relationship_type == ProvRevision:
            relationship.add_asserted_type(prov.model.PROV["Revision"])

        return relationship


@dataclass
class Model(abc.ABC):
    """
    Abstract base class for provenance submodels.

    Attributes:
        `context`
    """

    context: ProvContext = field(init=False)

    def __post_init__(self):
        self.context = ProvContext(prov.model.ProvDocument())

    @abc.abstractmethod
    def build(self) -> prov.model.ProvDocument:
        raise NotImplementedError


@dataclass
class PipelineVersionCreationModel(Model):
    """
    The provenance submodel for operator creation.

    Attributes:
        `pipeline_version_creation`
        `parent_pipeline_version_revision`
        `parent_pipeline_version_creation`
    """

    pipeline_version_creation: PipelineVersionCreation
    parent_pipeline_version_revision: PipelineVersionRevision | None
    parent_pipeline_version_creation: PipelineVersionCreation | None

    def build(self) -> prov.model.ProvDocument:
        # Add `PipelineVersionCreation`, parent `PipelineVersionCreation`, and relation
        self.context.add_element(self.pipeline_version_creation)
        if self.parent_pipeline_version_creation:
            self.context.add_element(self.parent_pipeline_version_creation)
            self.context.add_relation(
                self.pipeline_version_creation,
                self.parent_pipeline_version_creation,
                prov.model.ProvCommunication,
            )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = (
            self.pipeline_version_creation.pipeline_version_revision
        )
        self.context.add_element(pipeline_version_revision)
        for operator_revision in pipeline_version_revision.operators:
            self.context.add_element(operator_revision)
            self.context.add_relation(
                pipeline_version_revision,
                operator_revision,
                prov.model.ProvMembership,
            )
            # Also add `Operator` for initialization, make optional?
            operator = operator_revision.operator
            self.context.add_element(operator)
            self.context.add_relation(
                operator_revision,
                operator,
                prov.model.ProvSpecialization,
            )
        for connection in pipeline_version_revision.connections:
            self.context.add_element(connection)
            self.context.add_relation(
                pipeline_version_revision,
                connection,
                prov.model.ProvMembership,
            )
        self.context.add_relation(
            pipeline_version_revision,
            self.pipeline_version_creation,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_version_creation.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_PIPELINE_VERSION_REVISION,
            },
        )

        # Add parent `PipelineVersionRevision`, and relations
        if self.parent_pipeline_version_revision:
            self.context.add_element(self.parent_pipeline_version_revision)
            self.context.add_relation(
                pipeline_version_revision,
                self.parent_pipeline_version_revision,
                prov.model.ProvDerivation,
            )
            self.context.add_relation(
                self.pipeline_version_creation,
                self.parent_pipeline_version_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_version_creation.time,
                    str(
                        prov.model.PROV_ROLE
                    ): ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                },
            )

        # Add created `PipelineVersion`, parent `PipelineVersion`, and relations
        pipeline_version = pipeline_version_revision.pipeline_version
        self.context.add_element(pipeline_version)
        self.context.add_relation(
            pipeline_version_revision,
            pipeline_version_revision.pipeline_version,
            prov.model.ProvSpecialization,
        )
        self.context.add_relation(
            pipeline_version,
            self.pipeline_version_creation,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_version_creation.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_PIPELINE_VERSION,
            },
        )
        if self.parent_pipeline_version_creation:
            parent_pipeline_version = (
                self.parent_pipeline_version_creation.pipeline_version_revision.pipeline_version
            )
            self.context.add_element(parent_pipeline_version)
            self.context.add_relation(
                self.parent_pipeline_version_revision,
                parent_pipeline_version,
                prov.model.ProvSpecialization,
            )
            self.context.add_relation(
                pipeline_version,
                parent_pipeline_version,
                prov.model.ProvDerivation,
            )
            self.context.add_relation(
                self.pipeline_version_creation,
                parent_pipeline_version,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_version_creation.time,
                    str(prov.model.PROV_ROLE): ProvRole.USED_PARENT_PIPELINE_VERSION,
                },
            )

        return self.context.document


@dataclass
class OperatorCreationModel(Model):
    """
    The provenance submodel for operator creation.

    Attributes:
        `pipeline_change`
        `parent_pipeline_change`
        `parent_pipeline_version_revision`
    """

    pipeline_change: OperatorCreationPipelineChange
    parent_pipeline_change: PipelineChange | None
    parent_pipeline_version_revision: PipelineVersionRevision | None

    def build(self) -> prov.model.ProvDocument:
        # Add `PipelineChange`, parent `PipelineChange`, and relation
        self.context.add_element(self.pipeline_change)
        if self.parent_pipeline_change:
            self.context.add_element(self.parent_pipeline_change)
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_change,
                prov.model.ProvCommunication,
            )

        # Add created `Operator`, corresponding `OperatorRevision`, and relations
        operator_revision = self.pipeline_change.operator_revision
        self.context.add_element(operator_revision)
        self.context.add_relation(
            operator_revision,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_OPERATOR,
            },
        )
        operator = operator_revision.operator
        self.context.add_element(operator)
        self.context.add_relation(
            operator_revision,
            operator,
            prov.model.ProvSpecialization,
        )

        # Add operator parameters and relations
        for parameter in operator_revision.parameters:
            self.context.add_element(parameter)
            self.context.add_relation(
                operator_revision,
                parameter,
                prov.model.ProvMembership,
            )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = self.pipeline_change.pipeline_version_revision
        self.context.add_element(pipeline_version_revision)
        for operator_revision in pipeline_version_revision.operators:
            self.context.add_element(
                operator_revision,
            )
            self.context.add_relation(
                pipeline_version_revision,
                operator_revision,
                prov.model.ProvMembership,
            )
        for connection in pipeline_version_revision.connections:
            self.context.add_element(
                connection,
            )
            self.context.add_relation(
                pipeline_version_revision,
                connection,
                prov.model.ProvMembership,
            )
        self.context.add_relation(
            pipeline_version_revision,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_PIPELINE_VERSION_REVISION,
            },
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        self.context.add_element(pipeline_version_revision.pipeline_version)
        self.context.add_relation(
            pipeline_version_revision,
            pipeline_version_revision.pipeline_version,
            prov.model.ProvSpecialization,
        )
        if self.parent_pipeline_version_revision:
            self.context.add_element(self.parent_pipeline_version_revision)
            self.context.add_relation(
                pipeline_version_revision,
                self.parent_pipeline_version_revision,
                ProvRevision,
            )
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_version_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                    str(
                        prov.model.PROV_ROLE
                    ): ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                },
            )

        return self.context.document


@dataclass
class OperatorModificationModel(Model):
    """
    The provenance submodel for operator modification.

    Attributes:
        `pipeline_change`
        `parent_pipeline_change`
        `parent_operator_revision`
        `parent_pipeline_version_revision`
    """

    pipeline_change: OperatorModificationPipelineChange
    parent_pipeline_change: PipelineChange | None
    parent_operator_revision: OperatorRevision | None
    parent_pipeline_version_revision: PipelineVersionRevision | None

    def build(self) -> prov.model.ProvDocument:
        # Add `PipelineChange`, parent `PipelineChange`, and relation
        self.context.add_element(self.pipeline_change)
        if self.parent_pipeline_change:
            self.context.add_element(self.parent_pipeline_change)
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_change,
                prov.model.ProvCommunication,
            )

        # Add modified `Operator`, new `OperatorRevision`, parent `OperatorRevision` and relations
        operator_revision = self.pipeline_change.operator_revision
        self.context.add_element(operator_revision)
        self.context.add_relation(
            operator_revision,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.MODIFIED_OPERATOR,
            },
        )
        if self.parent_operator_revision:
            self.context.add_element(self.parent_operator_revision)
            self.context.add_relation(
                operator_revision,
                self.parent_operator_revision,
                ProvRevision,
            )
            self.context.add_relation(
                self.pipeline_change,
                self.parent_operator_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                    str(prov.model.PROV_ROLE): ProvRole.USED_PARENT_OPERATOR_REVISION,
                },
            )
        operator = operator_revision.operator
        self.context.add_element(operator)
        self.context.add_relation(
            operator_revision,
            operator,
            prov.model.ProvSpecialization,
        )

        # Add operator parameters and relations
        for parameter in operator_revision.parameters:
            self.context.add_element(parameter)
            self.context.add_relation(
                operator_revision,
                parameter,
                prov.model.ProvMembership,
            )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = self.pipeline_change.pipeline_version_revision
        self.context.add_element(pipeline_version_revision)
        for operator_revision in pipeline_version_revision.operators:
            self.context.add_element(
                operator_revision,
            )
            self.context.add_relation(
                pipeline_version_revision,
                operator_revision,
                prov.model.ProvMembership,
            )
        for connection in pipeline_version_revision.connections:
            self.context.add_element(
                connection,
            )
            self.context.add_relation(
                pipeline_version_revision,
                connection,
                prov.model.ProvMembership,
            )
        self.context.add_relation(
            pipeline_version_revision,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_PIPELINE_VERSION_REVISION,
            },
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        self.context.add_element(pipeline_version_revision.pipeline_version)
        self.context.add_relation(
            pipeline_version_revision,
            pipeline_version_revision.pipeline_version,
            prov.model.ProvSpecialization,
        )
        if self.parent_pipeline_version_revision:
            self.context.add_element(self.parent_pipeline_version_revision)
            self.context.add_relation(
                pipeline_version_revision,
                self.parent_pipeline_version_revision,
                ProvRevision,
            )
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_version_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                    str(
                        prov.model.PROV_ROLE
                    ): ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                },
            )

        return self.context.document


@dataclass
class OperatorDeletionModel(Model):
    """
    The provenance submodel for operator deletion.

    Attributes:
        `pipeline_change`
        `parent_pipeline_change`
        `parent_pipeline_version_revision`
    """

    pipeline_change: OperatorDeletionPipelineChange
    parent_pipeline_change: PipelineChange | None
    parent_pipeline_version_revision: PipelineVersionRevision | None

    def build(self) -> prov.model.ProvDocument:
        # Add `PipelineChange`, parent `PipelineChange`, and relation
        self.context.add_element(self.pipeline_change)
        if self.parent_pipeline_change:
            self.context.add_element(self.parent_pipeline_change)
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_change,
                prov.model.ProvCommunication,
            )

        # Add deleted `Operator`, corresponding `OperatorRevision`, and relations
        operator_revision = self.pipeline_change.operator_revision
        self.context.add_element(operator_revision)
        self.context.add_relation(
            operator_revision,
            self.pipeline_change,
            prov.model.ProvInvalidation,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.DELETED_OPERATOR,
            },
        )
        operator = operator_revision.operator
        self.context.add_element(operator)
        self.context.add_relation(
            operator_revision,
            operator,
            prov.model.ProvSpecialization,
        )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = self.pipeline_change.pipeline_version_revision
        self.context.add_element(pipeline_version_revision)
        for operator_revision in pipeline_version_revision.operators:
            self.context.add_element(
                operator_revision,
            )
            self.context.add_relation(
                pipeline_version_revision,
                operator_revision,
                prov.model.ProvMembership,
            )
        for connection in pipeline_version_revision.connections:
            self.context.add_element(
                connection,
            )
            self.context.add_relation(
                pipeline_version_revision,
                connection,
                prov.model.ProvMembership,
            )
        self.context.add_relation(
            pipeline_version_revision,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_PIPELINE_VERSION_REVISION,
            },
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        self.context.add_element(pipeline_version_revision.pipeline_version)
        self.context.add_relation(
            pipeline_version_revision,
            pipeline_version_revision.pipeline_version,
            prov.model.ProvSpecialization,
        )
        if self.parent_pipeline_version_revision:
            self.context.add_element(self.parent_pipeline_version_revision)
            self.context.add_relation(
                pipeline_version_revision,
                self.parent_pipeline_version_revision,
                ProvRevision,
            )
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_version_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                    str(
                        prov.model.PROV_ROLE
                    ): ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                },
            )

        return self.context.document


@dataclass
class ConnectionCreationModel(Model):
    """
    The provenance submodel for connection creation.

    Attributes:
        `pipeline_change`
        `parent_pipeline_change`
        `parent_pipeline_version_revision`
    """

    pipeline_change: ConnectionCreationPipelineChange
    parent_pipeline_change: PipelineChange | None
    parent_pipeline_version_revision: PipelineVersionRevision | None

    def build(self) -> prov.model.ProvDocument:
        # Add `PipelineChange`, parent `PipelineChange`, and relation
        self.context.add_element(self.pipeline_change)
        if self.parent_pipeline_change:
            self.context.add_element(self.parent_pipeline_change)
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_change,
                prov.model.ProvCommunication,
            )

        # Add created `Connection` and relation
        connection = self.pipeline_change.connection
        self.context.add_element(connection)
        self.context.add_relation(
            connection,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_CONNECTION,
            },
        )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = self.pipeline_change.pipeline_version_revision
        self.context.add_element(pipeline_version_revision)
        for operator_revision in pipeline_version_revision.operators:
            self.context.add_element(operator_revision)
            self.context.add_relation(
                pipeline_version_revision,
                operator_revision,
                prov.model.ProvMembership,
            )
        for connection in pipeline_version_revision.connections:
            self.context.add_element(connection)
            self.context.add_relation(
                pipeline_version_revision,
                connection,
                prov.model.ProvMembership,
            )
        self.context.add_relation(
            pipeline_version_revision,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_PIPELINE_VERSION_REVISION,
            },
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        self.context.add_element(pipeline_version_revision.pipeline_version)
        self.context.add_relation(
            pipeline_version_revision,
            pipeline_version_revision.pipeline_version,
            prov.model.ProvSpecialization,
        )
        if self.parent_pipeline_version_revision:
            self.context.add_element(self.parent_pipeline_version_revision)
            self.context.add_relation(
                pipeline_version_revision,
                self.parent_pipeline_version_revision,
                ProvRevision,
            )
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_version_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                    str(
                        prov.model.PROV_ROLE
                    ): ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                },
            )

        return self.context.document


@dataclass
class ConnectionDeletionModel(Model):
    """
    The provenance submodel for connection deletion.

    Attributes:
        `pipeline_change`
        `parent_pipeline_change`
        `parent_pipeline_version_revision`
    """

    pipeline_change: ConnectionDeletionPipelineChange
    parent_pipeline_change: PipelineChange | None
    parent_pipeline_version_revision: PipelineVersionRevision | None

    def build(self) -> prov.model.ProvDocument:
        # Add `PipelineChange`, parent `PipelineChange`, and relation
        self.context.add_element(self.pipeline_change)
        if self.parent_pipeline_change:
            self.context.add_element(self.parent_pipeline_change)
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_change,
                prov.model.ProvCommunication,
            )

        # Add deleted `Connection` and relation
        connection = self.pipeline_change.connection
        self.context.add_element(connection)
        self.context.add_relation(
            connection,
            self.pipeline_change,
            prov.model.ProvInvalidation,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.DELETED_CONNECTION,
            },
        )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = self.pipeline_change.pipeline_version_revision
        self.context.add_element(pipeline_version_revision)
        for operator_revision in pipeline_version_revision.operators:
            self.context.add_element(operator_revision)
            self.context.add_relation(
                pipeline_version_revision,
                operator_revision,
                prov.model.ProvMembership,
            )
        for connection in pipeline_version_revision.connections:
            self.context.add_element(connection)
            self.context.add_relation(
                pipeline_version_revision,
                connection,
                prov.model.ProvMembership,
            )
        self.context.add_relation(
            pipeline_version_revision,
            self.pipeline_change,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_PIPELINE_VERSION_REVISION,
            },
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        self.context.add_element(pipeline_version_revision.pipeline_version)
        self.context.add_relation(
            pipeline_version_revision,
            pipeline_version_revision.pipeline_version,
            prov.model.ProvSpecialization,
        )
        if self.parent_pipeline_version_revision:
            self.context.add_element(self.parent_pipeline_version_revision)
            self.context.add_relation(
                pipeline_version_revision,
                self.parent_pipeline_version_revision,
                ProvRevision,
            )
            self.context.add_relation(
                self.pipeline_change,
                self.parent_pipeline_version_revision,
                prov.model.ProvUsage,
                {
                    str(prov.model.PROV_ATTR_TIME): self.pipeline_change.time,
                    str(
                        prov.model.PROV_ROLE
                    ): ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                },
            )

        return self.context.document


@dataclass
class OperationExecutionModel(Model):
    """
    The provenance submodel for operation execution.

    Attributes:
        `operator_execution`
    """

    operator_execution: OperatorExecution

    def build(self) -> prov.model.ProvDocument:
        # Add `OperatorExecution`, `OperatorRevision`, and relation
        self.context.add_element(self.operator_execution)
        operator_revision = self.operator_execution.operator_revision
        # Add operator parameters and relations
        for parameter in operator_revision.parameters:
            self.context.add_element(parameter)
            self.context.add_relation(
                operator_revision,
                parameter,
                prov.model.ProvMembership,
            )
        self.context.add_element(operator_revision)
        self.context.add_relation(
            self.operator_execution,
            operator_revision,
            prov.model.ProvUsage,
            {
                str(prov.model.PROV_ATTR_TIME): self.operator_execution.time,
                str(prov.model.PROV_ROLE): ProvRole.USED_OPERATOR_REVISION,
            },
        )

        # Add `OperatorRun` and relation
        operator_run = self.operator_execution.operator_run
        self.context.add_element(operator_run)
        self.context.add_relation(
            operator_run,
            self.operator_execution,
            prov.model.ProvGeneration,
            {
                str(prov.model.PROV_ATTR_TIME): self.operator_execution.time,
                str(prov.model.PROV_ROLE): ProvRole.CREATED_OPERATOR_RUN,
            },
        )

        # Add `Metric`s and relations
        for metric in operator_run.metrics:
            self.context.add_element(metric)
            self.context.add_relation(
                operator_run,
                metric,
                prov.model.ProvMembership,
            )
            self.context.add_relation(
                operator_revision,
                metric,
                prov.model.ProvMembership,
            )

        return self.context.document
