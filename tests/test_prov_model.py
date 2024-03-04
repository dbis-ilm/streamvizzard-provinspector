import datetime
import random
import unittest
import uuid
from typing import Type

import prov.model

from provinspector.domain.constants import (
    OperatorStepType,
    PipelineChangeType,
    ProvRole,
)
from provinspector.domain.model import (
    Connection,
    ConnectionCreationPipelineChange,
    ConnectionDeletionPipelineChange,
    Metric,
    Operator,
    OperatorCreationPipelineChange,
    OperatorDeletionPipelineChange,
    OperatorExecution,
    OperatorModificationPipelineChange,
    OperatorRevision,
    OperatorRun,
    Parameter,
    PipelineChange,
    PipelineVersion,
    PipelineVersionCreation,
    PipelineVersionRevision,
)
from provinspector.prov.model import (
    DEFAULT_NAMESPACE,
    ConnectionCreationModel,
    ConnectionDeletionModel,
    OperationExecutionModel,
    OperatorCreationModel,
    OperatorDeletionModel,
    OperatorModificationModel,
    PipelineVersionCreationModel,
    ProvContext,
)

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
tomorrow = today + datetime.timedelta(days=1)


def create_pipeline_change(
    parent: PipelineChange | None,
    time: datetime.datetime,
) -> PipelineChange:
    pipeline_version = PipelineVersion(
        id_=0,
        parent_pipeline_version_id=None,
    )

    pipeline_version_revision = PipelineVersionRevision(
        uuid=str(uuid.uuid4()),
        id_=0,
        pipeline_version=pipeline_version,
        parent_pipeline_version_revision_uuid=None,
        operators=list(),
        connections=list(),
    )

    return PipelineChange(
        uuid=str(uuid.uuid4()),
        pipeline_change_type=PipelineChangeType.OPERATOR_CREATION,
        time=time,
        operator_revision=None,
        connection=None,
        pipeline_version_revision=pipeline_version_revision,
        parent_pipeline_change_uuid=parent.uuid if parent else None,
    )


def relation_qualified_name(
    source,
    target,
) -> prov.model.QualifiedName:
    return prov.model.QualifiedName(
        DEFAULT_NAMESPACE,
        f"relation:{source.identifier}:{target.identifier}",
    )


class TestProvContext:
    def test_add_element(self):
        case = unittest.TestCase()

        pipeline_change = create_pipeline_change(
            parent=None,
            time=today,
        )

        context = ProvContext(prov.model.ProvDocument())
        el: prov.model.ProvRecord = context.add_element(pipeline_change)

        el_expected: prov.model.ProvElement = pipeline_change.to_prov()

        assert el._prov_type == el_expected._prov_type
        assert el.identifier == el_expected.identifier
        case.assertCountEqual(el.attributes, el_expected.attributes)

    def test_add_relation(self):
        parent_pipeline_change = create_pipeline_change(
            parent=None,
            time=yesterday,
        )
        pipeline_change = create_pipeline_change(
            parent=parent_pipeline_change,
            time=today,
        )

        context: ProvContext = ProvContext(prov.model.ProvDocument())
        source = pipeline_change.to_prov()
        target = parent_pipeline_change.to_prov()

        relationship_type: Type[prov.model.ProvRelation] = prov.model.ProvCommunication
        attributes = dict()
        relationship = context.document.new_record(
            relationship_type._prov_type,
            prov.model.QualifiedName(
                DEFAULT_NAMESPACE, f"relation:{source.identifier}:{target.identifier}"
            ),
            {
                relationship_type.FORMAL_ATTRIBUTES[0]: source,  # type: ignore
                relationship_type.FORMAL_ATTRIBUTES[1]: target,  # type: ignore
            },
        )
        relationship.add_attributes(attributes=attributes)
        context.document.add_record(relationship)

        context_expected: ProvContext = ProvContext(prov.model.ProvDocument())
        context_expected.add_relation(
            source_dataclass_instance=pipeline_change,
            target_dataclass_instance=parent_pipeline_change,
            relationship_type=prov.model.ProvCommunication,
            attributes={},
        )

        assert context == context_expected


class TestPipelineVersionCreationModel:
    def build(
        self,
        pipeline_version_creation: PipelineVersionCreation,
        parent_pipeline_version_revision: PipelineVersionRevision | None,
        parent_pipeline_version_creation: PipelineVersionCreation | None,
    ) -> prov.model.ProvDocument:
        doc = prov.model.ProvDocument()

        # Add `PipelineVersionCreation`, parent `PipelineVersionCreation`, and relation
        pipeline_version_creation_activity = doc.activity(
            pipeline_version_creation.to_prov().identifier,
            pipeline_version_creation.to_prov().get_startTime(),
            pipeline_version_creation.to_prov().get_endTime(),
            pipeline_version_creation.to_prov().attributes,
        )
        if parent_pipeline_version_creation:
            parent_pipeline_version_creation_activity = doc.activity(
                parent_pipeline_version_creation.to_prov().identifier,
                parent_pipeline_version_creation.to_prov().get_startTime(),
                parent_pipeline_version_creation.to_prov().get_endTime(),
                parent_pipeline_version_creation.to_prov().attributes,
            )
            doc.wasInformedBy(
                informed=pipeline_version_creation_activity,
                informant=parent_pipeline_version_creation_activity,
                identifier=relation_qualified_name(
                    pipeline_version_creation_activity,
                    parent_pipeline_version_creation_activity,
                ),
            )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = pipeline_version_creation.pipeline_version_revision
        pipeline_version_revision_entity = doc.entity(
            identifier=pipeline_version_revision.to_prov().identifier,
            other_attributes=pipeline_version_revision.to_prov().attributes,
        )
        for operator_revision in pipeline_version_revision.operators:
            operator_revision_entity = doc.entity(
                identifier=operator_revision.to_prov().identifier,
                other_attributes=operator_revision.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=operator_revision_entity,
            )
        for connection in pipeline_version_revision.connections:
            connection_entity = doc.entity(
                identifier=connection.to_prov().identifier,
                other_attributes=connection.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=connection_entity,
            )
        doc.wasGeneratedBy(
            entity=pipeline_version_revision_entity,
            activity=pipeline_version_creation_activity,
            time=pipeline_version_creation_activity.get_startTime(),
            identifier=relation_qualified_name(
                pipeline_version_revision_entity, pipeline_version_creation_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_version_creation_activity.get_startTime(),
                ),
                (prov.model.PROV_ROLE, ProvRole.CREATED_PIPELINE_VERSION_REVISION),
            ],
        )

        # Add parent `PipelineVersionRevision`, and relations
        if parent_pipeline_version_revision:
            parent_pipeline_version_revision_entity = doc.entity(
                identifier=parent_pipeline_version_revision.to_prov().identifier,
                other_attributes=parent_pipeline_version_revision.to_prov().attributes,
            )
            doc.wasDerivedFrom(
                generatedEntity=pipeline_version_revision_entity,
                usedEntity=parent_pipeline_version_revision_entity,
                identifier=relation_qualified_name(
                    pipeline_version_revision_entity,
                    parent_pipeline_version_revision_entity,
                ),
            )
            doc.used(
                activity=pipeline_version_creation_activity,
                entity=parent_pipeline_version_revision_entity,
                time=pipeline_version_creation_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_version_creation_activity,
                    parent_pipeline_version_revision_entity,
                ),
                other_attributes=[
                    (
                        str(prov.model.PROV_ROLE),
                        ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                    )
                ],
            )

        # Add created `PipelineVersion`, parent `PipelineVersion`, and relations
        pipeline_version = pipeline_version_revision.pipeline_version
        pipeline_version_entity = doc.entity(
            identifier=pipeline_version.to_prov().identifier,
            other_attributes=pipeline_version.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=pipeline_version_revision_entity,
            generalEntity=pipeline_version_entity,
        )
        doc.wasGeneratedBy(
            entity=pipeline_version_entity,
            activity=pipeline_version_creation_activity,
            time=pipeline_version_creation_activity.get_startTime(),
            identifier=relation_qualified_name(
                pipeline_version_entity, pipeline_version_creation_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_version_creation_activity.get_startTime(),
                ),
                (prov.model.PROV_ROLE, ProvRole.CREATED_PIPELINE_VERSION),
            ],
        )
        if parent_pipeline_version_creation:
            parent_pipeline_version = (
                parent_pipeline_version_creation.pipeline_version_revision.pipeline_version
            )
            if pipeline_version == parent_pipeline_version:
                parent_pipeline_version_entity = pipeline_version_entity
            else:
                parent_pipeline_version_entity = doc.entity(
                    identifier=parent_pipeline_version.to_prov().identifier,
                    other_attributes=parent_pipeline_version.to_prov().attributes,
                )
            if parent_pipeline_version_revision:
                # parent_pipeline_version_revision_entity = doc.entity(
                #     identifier=parent_pipeline_version_revision.to_prov().identifier,
                #     other_attributes=parent_pipeline_version_revision.to_prov().attributes,
                # )
                doc.specializationOf(
                    specificEntity=parent_pipeline_version_revision_entity,
                    generalEntity=parent_pipeline_version_entity,
                )
            doc.wasDerivedFrom(
                generatedEntity=pipeline_version_entity,
                usedEntity=parent_pipeline_version_entity,
                identifier=relation_qualified_name(
                    pipeline_version_entity,
                    parent_pipeline_version_entity,
                ),
            )
            doc.used(
                activity=pipeline_version_creation_activity,
                entity=parent_pipeline_version_entity,
                time=pipeline_version_creation_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_version_creation_activity,
                    parent_pipeline_version_entity,
                ),
                other_attributes=[
                    (
                        str(prov.model.PROV_ROLE),
                        ProvRole.USED_PARENT_PIPELINE_VERSION,
                    )
                ],
            )

        return doc

    def test_build(self):
        connection = Connection(
            id_=0,
            from_operator_id=0,
            to_operator_id=1,
        )
        operator = Operator(
            id_=0,
            name=str(uuid.uuid4()),
        )
        parameter = Parameter(
            name=str(uuid.uuid4()),
            value=random.uniform(0, 1),
        )
        operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            operator=operator,
            parameters=[parameter],
            parent_operator_revision_uuid=None,
        )
        pipeline_version = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        parent_pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=None,
            operators=list(),
            connections=list(),
        )
        parent_pipeline_version_creation = PipelineVersionCreation(
            uuid=str(uuid.uuid4()),
            pipeline_version_revision=parent_pipeline_version_revision,
            parent_pipeline_version_creation_uuid=None,
            time=today,
        )
        pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=1,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
            operators=[operator_revision],
            connections=[connection],
        )
        pipeline_version_creation = PipelineVersionCreation(
            uuid=str(uuid.uuid4()),
            pipeline_version_revision=pipeline_version_revision,
            parent_pipeline_version_creation_uuid=parent_pipeline_version_creation.uuid,
            time=today,
        )
        model = PipelineVersionCreationModel(
            pipeline_version_creation=pipeline_version_creation,
            parent_pipeline_version_revision=parent_pipeline_version_revision,
            parent_pipeline_version_creation=parent_pipeline_version_creation,
        )

        assert (
            self.build(
                pipeline_version_creation=pipeline_version_creation,
                parent_pipeline_version_revision=parent_pipeline_version_revision,
                parent_pipeline_version_creation=parent_pipeline_version_creation,
            )
            == model.build()
        )


class TestOperatorCreationModel:
    def build(
        self,
        pipeline_change: OperatorCreationPipelineChange,
        parent_pipeline_change: PipelineChange | None,
        parent_pipeline_version_revision: PipelineVersionRevision | None,
    ) -> prov.model.ProvDocument:
        doc = prov.model.ProvDocument()

        # Add `PipelineChange`, parent `PipelineChange`, and relation
        pipeline_change_activity = doc.activity(
            pipeline_change.to_prov().identifier,
            pipeline_change.to_prov().get_startTime(),
            pipeline_change.to_prov().get_endTime(),
            pipeline_change.to_prov().attributes,
        )
        if parent_pipeline_change:
            parent_pipeline_change_activity = doc.activity(
                parent_pipeline_change.to_prov().identifier,
                parent_pipeline_change.to_prov().get_startTime(),
                parent_pipeline_change.to_prov().get_endTime(),
                parent_pipeline_change.to_prov().attributes,
            )
            doc.wasInformedBy(
                informed=pipeline_change_activity,
                informant=parent_pipeline_change_activity,
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_change_activity,
                ),
            )

        # Add created `Operator`, corresponding `OperatorRevision`, and relations
        operator_revision = pipeline_change.operator_revision
        operator_revision_entity = doc.entity(
            identifier=operator_revision.to_prov().identifier,
            other_attributes=operator_revision.to_prov().attributes,
        )
        doc.wasGeneratedBy(
            entity=operator_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                operator_revision_entity,
                pipeline_change_activity,
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (prov.model.PROV_ROLE, ProvRole.CREATED_OPERATOR),
            ],
        )
        operator = operator_revision.operator
        operator_entity = doc.entity(
            identifier=operator.to_prov().identifier,
            other_attributes=operator.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=operator_revision_entity,
            generalEntity=operator_entity,
        )

        # Add operator parameters and relations
        for parameter in operator_revision.parameters:
            parameter_entity = doc.entity(
                identifier=parameter.to_prov().identifier,
                other_attributes=parameter.to_prov().attributes,
            )
            doc.hadMember(
                collection=operator_revision_entity,
                entity=parameter_entity,
            )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = pipeline_change.pipeline_version_revision
        pipeline_version_revision_entity = doc.entity(
            identifier=pipeline_version_revision.to_prov().identifier,
            other_attributes=pipeline_version_revision.to_prov().attributes,
        )
        for operator_revision in pipeline_version_revision.operators:
            operator_revision_entity = doc.entity(
                identifier=operator_revision.to_prov().identifier,
                other_attributes=operator_revision.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=operator_revision_entity,
            )
        for connection in pipeline_version_revision.connections:
            connection_entity = doc.entity(
                identifier=connection.to_prov().identifier,
                other_attributes=connection.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=connection_entity,
            )
        doc.wasGeneratedBy(
            entity=pipeline_version_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                pipeline_version_revision_entity, pipeline_change_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (prov.model.PROV_ROLE, ProvRole.CREATED_PIPELINE_VERSION_REVISION),
            ],
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        pipeline_version = pipeline_version_revision.pipeline_version
        pipeline_version_entity = doc.entity(
            identifier=pipeline_version.to_prov().identifier,
            other_attributes=pipeline_version.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=pipeline_version_revision_entity,
            generalEntity=pipeline_version_entity,
        )
        if parent_pipeline_version_revision:
            parent_pipeline_version_revision_entity = doc.entity(
                identifier=parent_pipeline_version_revision.to_prov().identifier,
                other_attributes=parent_pipeline_version_revision.to_prov().attributes,
            )
            doc.wasRevisionOf(
                generatedEntity=pipeline_version_revision_entity,
                usedEntity=parent_pipeline_version_revision_entity,
                identifier=relation_qualified_name(
                    pipeline_version_revision_entity,
                    parent_pipeline_version_revision_entity,
                ),
            )
            doc.used(
                activity=pipeline_change_activity,
                entity=parent_pipeline_version_revision_entity,
                time=pipeline_change_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_version_revision_entity,
                ),
                other_attributes=[
                    (
                        str(prov.model.PROV_ROLE),
                        ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                    )
                ],
            )

        return doc

    def test_build(self):
        connection = Connection(
            id_=0,
            from_operator_id=0,
            to_operator_id=1,
        )
        operator = Operator(
            id_=0,
            name=str(uuid.uuid4()),
        )
        parameter = Parameter(
            name=str(uuid.uuid4()),
            value=random.uniform(0, 1),
        )
        operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            operator=operator,
            parameters=[parameter],
            parent_operator_revision_uuid=None,
        )
        pipeline_version = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        parent_pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=None,
            operators=list(),
            connections=list(),
        )
        pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=1,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
            operators=[operator_revision],
            connections=[connection],
        )
        parent_pipeline_change = PipelineChange(
            uuid=str(uuid.uuid4()),
            pipeline_change_type=PipelineChangeType.CONNECTION_CREATION,
            time=today,
            operator_revision=None,
            connection=None,
            pipeline_version_revision=parent_pipeline_version_revision,
            parent_pipeline_change_uuid=None,
        )
        pipeline_change = OperatorCreationPipelineChange(
            uuid=str(uuid.uuid4()),
            time=today,
            operator_revision=operator_revision,
            pipeline_version_revision=pipeline_version_revision,
            parent_pipeline_change_uuid=parent_pipeline_change.uuid,
        )
        model = OperatorCreationModel(
            pipeline_change=pipeline_change,
            parent_pipeline_change=parent_pipeline_change,
            parent_pipeline_version_revision=parent_pipeline_version_revision,
        )

        assert (
            self.build(
                pipeline_change=pipeline_change,
                parent_pipeline_change=parent_pipeline_change,
                parent_pipeline_version_revision=parent_pipeline_version_revision,
            )
            == model.build()
        )


class TestOperatorModificationModel:
    def build(
        self,
        pipeline_change: OperatorModificationPipelineChange,
        parent_pipeline_change: PipelineChange | None,
        parent_operator_revision: OperatorRevision | None,
        parent_pipeline_version_revision: PipelineVersionRevision | None,
    ) -> prov.model.ProvDocument:
        doc = prov.model.ProvDocument()

        # Add `PipelineChange`, parent `PipelineChange`, and relation
        pipeline_change_activity = doc.activity(
            pipeline_change.to_prov().identifier,
            pipeline_change.to_prov().get_startTime(),
            pipeline_change.to_prov().get_endTime(),
            pipeline_change.to_prov().attributes,
        )
        if parent_pipeline_change:
            parent_pipeline_change_activity = doc.activity(
                parent_pipeline_change.to_prov().identifier,
                parent_pipeline_change.to_prov().get_startTime(),
                parent_pipeline_change.to_prov().get_endTime(),
                parent_pipeline_change.to_prov().attributes,
            )
            doc.wasInformedBy(
                informed=pipeline_change_activity,
                informant=parent_pipeline_change_activity,
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_change_activity,
                ),
            )

        # Add modified `Operator`, new `OperatorRevision`, parent `OperatorRevision` and relations
        operator_revision = pipeline_change.operator_revision
        operator_revision_entity = doc.entity(
            identifier=operator_revision.to_prov().identifier,
            other_attributes=operator_revision.to_prov().attributes,
        )
        doc.wasGeneratedBy(
            entity=operator_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                operator_revision_entity,
                pipeline_change_activity,
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.MODIFIED_OPERATOR),
            ],
        )
        if parent_operator_revision:
            parent_operator_revision_entity = doc.entity(
                identifier=parent_operator_revision.to_prov().identifier,
                other_attributes=parent_operator_revision.to_prov().attributes,
            )
            doc.wasRevisionOf(
                generatedEntity=operator_revision_entity,
                usedEntity=parent_operator_revision_entity,
                identifier=relation_qualified_name(
                    operator_revision_entity,
                    parent_operator_revision_entity,
                ),
            )
            doc.used(
                activity=pipeline_change_activity,
                entity=parent_operator_revision_entity,
                time=pipeline_change_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_change_activity, parent_operator_revision_entity
                ),
                other_attributes=[
                    (str(prov.model.PROV_ROLE), ProvRole.USED_PARENT_OPERATOR_REVISION)
                ],
            )
        operator = operator_revision.operator
        operator_entity = doc.entity(
            identifier=operator.to_prov().identifier,
            other_attributes=operator.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=operator_revision_entity,
            generalEntity=operator_entity,
        )

        # Add operator parameters and relations
        for parameter in operator_revision.parameters:
            parameter_entity = doc.entity(
                identifier=parameter.to_prov().identifier,
                other_attributes=parameter.to_prov().attributes,
            )
            doc.hadMember(
                collection=operator_revision_entity,
                entity=parameter_entity,
            )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = pipeline_change.pipeline_version_revision
        pipeline_version_revision_entity = doc.entity(
            identifier=pipeline_version_revision.to_prov().identifier,
            other_attributes=pipeline_version_revision.to_prov().attributes,
        )
        for operator_revision in pipeline_version_revision.operators:
            operator_revision_entity = doc.entity(
                identifier=operator_revision.to_prov().identifier,
                other_attributes=operator_revision.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=operator_revision_entity,
            )
        for connection in pipeline_version_revision.connections:
            connection_entity = doc.entity(
                identifier=connection.to_prov().identifier,
                other_attributes=connection.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=connection_entity,
            )
        doc.wasGeneratedBy(
            entity=pipeline_version_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                pipeline_version_revision_entity, pipeline_change_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.CREATED_PIPELINE_VERSION_REVISION),
            ],
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        pipeline_version = pipeline_version_revision.pipeline_version
        pipeline_version_entity = doc.entity(
            identifier=pipeline_version.to_prov().identifier,
            other_attributes=pipeline_version.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=pipeline_version_revision_entity,
            generalEntity=pipeline_version_entity,
        )
        if parent_pipeline_version_revision:
            parent_pipeline_version_revision_entity = doc.entity(
                identifier=parent_pipeline_version_revision.to_prov().identifier,
                other_attributes=parent_pipeline_version_revision.to_prov().attributes,
            )
            doc.wasRevisionOf(
                generatedEntity=pipeline_version_revision_entity,
                usedEntity=parent_pipeline_version_revision_entity,
                identifier=relation_qualified_name(
                    pipeline_version_revision_entity,
                    parent_pipeline_version_revision_entity,
                ),
            )
            doc.used(
                activity=pipeline_change_activity,
                entity=parent_pipeline_version_revision_entity,
                time=pipeline_change_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_version_revision_entity,
                ),
                other_attributes=[
                    (
                        str(prov.model.PROV_ROLE),
                        ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                    )
                ],
            )

        return doc

    def test_build(self):
        connection = Connection(
            id_=0,
            from_operator_id=0,
            to_operator_id=1,
        )
        operator = Operator(
            id_=0,
            name=str(uuid.uuid4()),
        )
        parameter = Parameter(
            name=str(uuid.uuid4()),
            value=random.uniform(0, 1),
        )
        parent_operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            operator=operator,
            parameters=[parameter],
            parent_operator_revision_uuid=None,
        )
        operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=1,
            operator=operator,
            parameters=[parameter],
            parent_operator_revision_uuid=parent_operator_revision.uuid,
        )
        pipeline_version = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        parent_pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=None,
            operators=list(),
            connections=list(),
        )
        pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=1,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
            operators=[operator_revision],
            connections=[connection],
        )
        parent_pipeline_change = PipelineChange(
            uuid=str(uuid.uuid4()),
            pipeline_change_type=PipelineChangeType.CONNECTION_CREATION,
            time=today,
            operator_revision=None,
            connection=None,
            pipeline_version_revision=parent_pipeline_version_revision,
            parent_pipeline_change_uuid=None,
        )
        pipeline_change = OperatorModificationPipelineChange(
            uuid=str(uuid.uuid4()),
            time=today,
            operator_revision=operator_revision,
            pipeline_version_revision=pipeline_version_revision,
            parent_pipeline_change_uuid=parent_pipeline_change.uuid,
        )
        model = OperatorModificationModel(
            pipeline_change=pipeline_change,
            parent_pipeline_change=parent_pipeline_change,
            parent_operator_revision=parent_operator_revision,
            parent_pipeline_version_revision=parent_pipeline_version_revision,
        )

        assert (
            self.build(
                pipeline_change=pipeline_change,
                parent_pipeline_change=parent_pipeline_change,
                parent_operator_revision=parent_operator_revision,
                parent_pipeline_version_revision=parent_pipeline_version_revision,
            )
            == model.build()
        )


class TestOperatorDeletionModel:
    def build(
        self,
        pipeline_change: OperatorDeletionPipelineChange,
        parent_pipeline_change: PipelineChange | None,
        parent_pipeline_version_revision: PipelineVersionRevision | None,
    ) -> prov.model.ProvDocument:
        doc = prov.model.ProvDocument()

        # Add `PipelineChange`, parent `PipelineChange`, and relation
        pipeline_change_activity = doc.activity(
            pipeline_change.to_prov().identifier,
            pipeline_change.to_prov().get_startTime(),
            pipeline_change.to_prov().get_endTime(),
            pipeline_change.to_prov().attributes,
        )
        if parent_pipeline_change:
            parent_pipeline_change_activity = doc.activity(
                parent_pipeline_change.to_prov().identifier,
                parent_pipeline_change.to_prov().get_startTime(),
                parent_pipeline_change.to_prov().get_endTime(),
                parent_pipeline_change.to_prov().attributes,
            )
            doc.wasInformedBy(
                informed=pipeline_change_activity,
                informant=parent_pipeline_change_activity,
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_change_activity,
                ),
            )

        # Add deleted `Operator`, corresponding `OperatorRevision`, and relations
        operator_revision = pipeline_change.operator_revision
        operator_revision_entity = doc.entity(
            identifier=operator_revision.to_prov().identifier,
            other_attributes=operator_revision.to_prov().attributes,
        )
        doc.wasInvalidatedBy(
            entity=operator_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                operator_revision_entity,
                pipeline_change_activity,
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.DELETED_OPERATOR),
            ],
        )
        operator = operator_revision.operator
        operator_entity = doc.entity(
            identifier=operator.to_prov().identifier,
            other_attributes=operator.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=operator_revision_entity,
            generalEntity=operator_entity,
        )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = pipeline_change.pipeline_version_revision
        pipeline_version_revision_entity = doc.entity(
            identifier=pipeline_version_revision.to_prov().identifier,
            other_attributes=pipeline_version_revision.to_prov().attributes,
        )
        for operator_revision in pipeline_version_revision.operators:
            operator_revision_entity = doc.entity(
                identifier=operator_revision.to_prov().identifier,
                other_attributes=operator_revision.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=operator_revision_entity,
            )
        for connection in pipeline_version_revision.connections:
            connection_entity = doc.entity(
                identifier=connection.to_prov().identifier,
                other_attributes=connection.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=connection_entity,
            )
        doc.wasGeneratedBy(
            entity=pipeline_version_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                pipeline_version_revision_entity, pipeline_change_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.CREATED_PIPELINE_VERSION_REVISION),
            ],
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        pipeline_version = pipeline_version_revision.pipeline_version
        pipeline_version_entity = doc.entity(
            identifier=pipeline_version.to_prov().identifier,
            other_attributes=pipeline_version.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=pipeline_version_revision_entity,
            generalEntity=pipeline_version_entity,
        )
        if parent_pipeline_version_revision:
            parent_pipeline_version_revision_entity = doc.entity(
                identifier=parent_pipeline_version_revision.to_prov().identifier,
                other_attributes=parent_pipeline_version_revision.to_prov().attributes,
            )
            doc.wasRevisionOf(
                generatedEntity=pipeline_version_revision_entity,
                usedEntity=parent_pipeline_version_revision_entity,
                identifier=relation_qualified_name(
                    pipeline_version_revision_entity,
                    parent_pipeline_version_revision_entity,
                ),
            )
            doc.used(
                activity=pipeline_change_activity,
                entity=parent_pipeline_version_revision_entity,
                time=pipeline_change_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_version_revision_entity,
                ),
                other_attributes=[
                    (
                        str(prov.model.PROV_ROLE),
                        ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                    )
                ],
            )

        return doc

    def test_build(self):
        connection = Connection(
            id_=0,
            from_operator_id=0,
            to_operator_id=1,
        )
        operator = Operator(
            id_=0,
            name=str(uuid.uuid4()),
        )
        parameter = Parameter(
            name=str(uuid.uuid4()),
            value=random.uniform(0, 1),
        )
        operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            operator=operator,
            parameters=[parameter],
            parent_operator_revision_uuid=None,
        )
        pipeline_version = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        parent_pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=None,
            operators=list(),
            connections=list(),
        )
        pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=1,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
            operators=[operator_revision],
            connections=[connection],
        )
        parent_pipeline_change = PipelineChange(
            uuid=str(uuid.uuid4()),
            pipeline_change_type=PipelineChangeType.CONNECTION_CREATION,
            time=today,
            operator_revision=None,
            connection=None,
            pipeline_version_revision=parent_pipeline_version_revision,
            parent_pipeline_change_uuid=None,
        )
        pipeline_change = OperatorDeletionPipelineChange(
            uuid=str(uuid.uuid4()),
            time=today,
            operator_revision=operator_revision,
            pipeline_version_revision=pipeline_version_revision,
            parent_pipeline_change_uuid=parent_pipeline_change.uuid,
        )
        model = OperatorDeletionModel(
            pipeline_change=pipeline_change,
            parent_pipeline_change=parent_pipeline_change,
            parent_pipeline_version_revision=parent_pipeline_version_revision,
        )

        assert (
            self.build(
                pipeline_change=pipeline_change,
                parent_pipeline_change=parent_pipeline_change,
                parent_pipeline_version_revision=parent_pipeline_version_revision,
            )
            == model.build()
        )


class TestConnectionCreationModel:
    def build(
        self,
        pipeline_change: ConnectionCreationPipelineChange,
        parent_pipeline_change: PipelineChange | None,
        parent_pipeline_version_revision: PipelineVersionRevision | None,
    ) -> prov.model.ProvDocument:
        doc = prov.model.ProvDocument()

        # Add `PipelineChange`, parent `PipelineChange`, and relation
        pipeline_change_activity = doc.activity(
            pipeline_change.to_prov().identifier,
            pipeline_change.to_prov().get_startTime(),
            pipeline_change.to_prov().get_endTime(),
            pipeline_change.to_prov().attributes,
        )
        if parent_pipeline_change:
            parent_pipeline_change_activity = doc.activity(
                parent_pipeline_change.to_prov().identifier,
                parent_pipeline_change.to_prov().get_startTime(),
                parent_pipeline_change.to_prov().get_endTime(),
                parent_pipeline_change.to_prov().attributes,
            )
            doc.wasInformedBy(
                informed=pipeline_change_activity,
                informant=parent_pipeline_change_activity,
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_change_activity,
                ),
            )

        # Add created `Connection` and relation
        connection = pipeline_change.connection
        connection_entity = doc.entity(
            identifier=connection.to_prov().identifier,
            other_attributes=connection.to_prov().attributes,
        )
        doc.wasGeneratedBy(
            entity=connection_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                connection_entity,
                pipeline_change_activity,
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.CREATED_CONNECTION),
            ],
        )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = pipeline_change.pipeline_version_revision
        pipeline_version_revision_entity = doc.entity(
            identifier=pipeline_version_revision.to_prov().identifier,
            other_attributes=pipeline_version_revision.to_prov().attributes,
        )
        for operator_revision in pipeline_version_revision.operators:
            operator_revision_entity = doc.entity(
                identifier=operator_revision.to_prov().identifier,
                other_attributes=operator_revision.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=operator_revision_entity,
            )
        for connection in pipeline_version_revision.connections:
            connection_entity = doc.entity(
                identifier=connection.to_prov().identifier,
                other_attributes=connection.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=connection_entity,
            )
        doc.wasGeneratedBy(
            entity=pipeline_version_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                pipeline_version_revision_entity, pipeline_change_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.CREATED_PIPELINE_VERSION_REVISION),
            ],
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        pipeline_version = pipeline_version_revision.pipeline_version
        pipeline_version_entity = doc.entity(
            identifier=pipeline_version.to_prov().identifier,
            other_attributes=pipeline_version.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=pipeline_version_revision_entity,
            generalEntity=pipeline_version_entity,
        )
        if parent_pipeline_version_revision:
            parent_pipeline_version_revision_entity = doc.entity(
                identifier=parent_pipeline_version_revision.to_prov().identifier,
                other_attributes=parent_pipeline_version_revision.to_prov().attributes,
            )
            doc.wasRevisionOf(
                generatedEntity=pipeline_version_revision_entity,
                usedEntity=parent_pipeline_version_revision_entity,
                identifier=relation_qualified_name(
                    pipeline_version_revision_entity,
                    parent_pipeline_version_revision_entity,
                ),
            )
            doc.used(
                activity=pipeline_change_activity,
                entity=parent_pipeline_version_revision_entity,
                time=pipeline_change_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_version_revision_entity,
                ),
                other_attributes=[
                    (
                        str(prov.model.PROV_ROLE),
                        ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                    )
                ],
            )

        return doc

    def test_build(self):
        connection = Connection(
            id_=0,
            from_operator_id=0,
            to_operator_id=1,
        )
        operator = Operator(
            id_=0,
            name=str(uuid.uuid4()),
        )
        operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            operator=operator,
            parameters=list(),
            parent_operator_revision_uuid=None,
        )
        pipeline_version = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        parent_pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=None,
            operators=list(),
            connections=list(),
        )
        pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=1,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
            operators=[operator_revision],
            connections=[connection],
        )
        parent_pipeline_change = PipelineChange(
            uuid=str(uuid.uuid4()),
            pipeline_change_type=PipelineChangeType.CONNECTION_CREATION,
            time=today,
            operator_revision=None,
            connection=None,
            pipeline_version_revision=parent_pipeline_version_revision,
            parent_pipeline_change_uuid=None,
        )
        pipeline_change = ConnectionCreationPipelineChange(
            uuid=str(uuid.uuid4()),
            time=today,
            connection=connection,
            pipeline_version_revision=pipeline_version_revision,
            parent_pipeline_change_uuid=parent_pipeline_change.uuid,
        )
        model = ConnectionCreationModel(
            pipeline_change=pipeline_change,
            parent_pipeline_change=parent_pipeline_change,
            parent_pipeline_version_revision=parent_pipeline_version_revision,
        )

        assert (
            self.build(
                pipeline_change=pipeline_change,
                parent_pipeline_change=parent_pipeline_change,
                parent_pipeline_version_revision=parent_pipeline_version_revision,
            )
            == model.build()
        )


class TestConnectionDeletionModel:
    def build(
        self,
        pipeline_change: ConnectionDeletionPipelineChange,
        parent_pipeline_change: PipelineChange | None,
        parent_pipeline_version_revision: PipelineVersionRevision | None,
    ) -> prov.model.ProvDocument:
        doc = prov.model.ProvDocument()

        # Add `PipelineChange`, parent `PipelineChange`, and relation
        pipeline_change_activity = doc.activity(
            pipeline_change.to_prov().identifier,
            pipeline_change.to_prov().get_startTime(),
            pipeline_change.to_prov().get_endTime(),
            pipeline_change.to_prov().attributes,
        )
        if parent_pipeline_change:
            parent_pipeline_change_activity = doc.activity(
                parent_pipeline_change.to_prov().identifier,
                parent_pipeline_change.to_prov().get_startTime(),
                parent_pipeline_change.to_prov().get_endTime(),
                parent_pipeline_change.to_prov().attributes,
            )
            doc.wasInformedBy(
                informed=pipeline_change_activity,
                informant=parent_pipeline_change_activity,
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_change_activity,
                ),
            )

        # Add deleted `Connection` and relation
        connection = pipeline_change.connection
        connection_entity = doc.entity(
            identifier=connection.to_prov().identifier,
            other_attributes=connection.to_prov().attributes,
        )
        doc.wasInvalidatedBy(
            entity=connection_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                connection_entity,
                pipeline_change_activity,
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.DELETED_CONNECTION),
            ],
        )

        # Add `PipelineVersionRevision`, corresponding `OperatorRevision` and `Connection` members, and relations
        pipeline_version_revision = pipeline_change.pipeline_version_revision
        pipeline_version_revision_entity = doc.entity(
            identifier=pipeline_version_revision.to_prov().identifier,
            other_attributes=pipeline_version_revision.to_prov().attributes,
        )
        for operator_revision in pipeline_version_revision.operators:
            operator_revision_entity = doc.entity(
                identifier=operator_revision.to_prov().identifier,
                other_attributes=operator_revision.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=operator_revision_entity,
            )
        for connection in pipeline_version_revision.connections:
            connection_entity = doc.entity(
                identifier=connection.to_prov().identifier,
                other_attributes=connection.to_prov().attributes,
            )
            doc.hadMember(
                collection=pipeline_version_revision_entity,
                entity=connection_entity,
            )
        doc.wasGeneratedBy(
            entity=pipeline_version_revision_entity,
            activity=pipeline_change_activity,
            time=pipeline_change_activity.get_startTime(),
            identifier=relation_qualified_name(
                pipeline_version_revision_entity, pipeline_change_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    pipeline_change_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.CREATED_PIPELINE_VERSION_REVISION),
            ],
        )

        # Add `PipelineVersion`, parent `PipelineVersionRevision`, and relations
        pipeline_version = pipeline_version_revision.pipeline_version
        pipeline_version_entity = doc.entity(
            identifier=pipeline_version.to_prov().identifier,
            other_attributes=pipeline_version.to_prov().attributes,
        )
        doc.specializationOf(
            specificEntity=pipeline_version_revision_entity,
            generalEntity=pipeline_version_entity,
        )
        if parent_pipeline_version_revision:
            parent_pipeline_version_revision_entity = doc.entity(
                identifier=parent_pipeline_version_revision.to_prov().identifier,
                other_attributes=parent_pipeline_version_revision.to_prov().attributes,
            )
            doc.wasRevisionOf(
                generatedEntity=pipeline_version_revision_entity,
                usedEntity=parent_pipeline_version_revision_entity,
                identifier=relation_qualified_name(
                    pipeline_version_revision_entity,
                    parent_pipeline_version_revision_entity,
                ),
            )
            doc.used(
                activity=pipeline_change_activity,
                entity=parent_pipeline_version_revision_entity,
                time=pipeline_change_activity.get_startTime(),
                identifier=relation_qualified_name(
                    pipeline_change_activity,
                    parent_pipeline_version_revision_entity,
                ),
                other_attributes=[
                    (
                        str(prov.model.PROV_ROLE),
                        ProvRole.USED_PARENT_PIPELINE_VERSION_REVISION,
                    )
                ],
            )

        return doc

    def test_build(self):
        connection = Connection(
            id_=0,
            from_operator_id=0,
            to_operator_id=1,
        )
        operator = Operator(
            id_=0,
            name=str(uuid.uuid4()),
        )
        operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            operator=operator,
            parameters=list(),
            parent_operator_revision_uuid=None,
        )
        pipeline_version = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        parent_pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=None,
            operators=list(),
            connections=list(),
        )
        pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=1,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
            operators=[operator_revision],
            connections=[connection],
        )
        parent_pipeline_change = PipelineChange(
            uuid=str(uuid.uuid4()),
            pipeline_change_type=PipelineChangeType.CONNECTION_DELETION,
            time=today,
            operator_revision=None,
            connection=None,
            pipeline_version_revision=parent_pipeline_version_revision,
            parent_pipeline_change_uuid=None,
        )
        pipeline_change = ConnectionDeletionPipelineChange(
            uuid=str(uuid.uuid4()),
            time=today,
            connection=connection,
            pipeline_version_revision=pipeline_version_revision,
            parent_pipeline_change_uuid=parent_pipeline_change.uuid,
        )
        model = ConnectionDeletionModel(
            pipeline_change=pipeline_change,
            parent_pipeline_change=parent_pipeline_change,
            parent_pipeline_version_revision=parent_pipeline_version_revision,
        )

        assert (
            self.build(
                pipeline_change=pipeline_change,
                parent_pipeline_change=parent_pipeline_change,
                parent_pipeline_version_revision=parent_pipeline_version_revision,
            )
            == model.build()
        )


class TestOperationExecutionModel:
    def build(
        self,
        operator_execution: OperatorExecution,
    ) -> prov.model.ProvDocument:
        doc = prov.model.ProvDocument()

        # Add `OperatorExecution`, `OperatorRevision`, and relation
        operator_execution_activity = doc.activity(
            operator_execution.to_prov().identifier,
            operator_execution.to_prov().get_startTime(),
            operator_execution.to_prov().get_endTime(),
            operator_execution.to_prov().attributes,
        )
        operator_revision = operator_execution.operator_revision
        operator_revision_entity = doc.entity(
            identifier=operator_revision.to_prov().identifier,
            other_attributes=operator_revision.to_prov().attributes,
        )
        doc.used(
            activity=operator_execution_activity,
            entity=operator_revision_entity,
            time=operator_execution_activity.get_startTime(),
            identifier=relation_qualified_name(
                operator_execution_activity, operator_revision_entity
            ),
            other_attributes=[
                (str(prov.model.PROV_ROLE), ProvRole.USED_OPERATOR_REVISION)
            ],
        )

        # Add `OperatorRun` and relation
        operator_run = operator_execution.operator_run
        operator_run_entity = doc.entity(
            identifier=operator_run.to_prov().identifier,
            other_attributes=operator_run.to_prov().attributes,
        )
        doc.wasGeneratedBy(
            entity=operator_run_entity,
            activity=operator_execution_activity,
            time=operator_execution_activity.get_startTime(),
            identifier=relation_qualified_name(
                operator_run_entity, operator_execution_activity
            ),
            other_attributes=[
                (
                    str(prov.model.PROV_ATTR_TIME),
                    operator_execution_activity.get_startTime(),
                ),
                (str(prov.model.PROV_ROLE), ProvRole.CREATED_OPERATOR_RUN),
            ],
        )

        # Add `Metric`s and relations
        for metric in operator_run.metrics:
            metric_entity = doc.entity(
                identifier=metric.to_prov().identifier,
                other_attributes=metric.to_prov().attributes,
            )
            doc.hadMember(
                collection=operator_run_entity,
                entity=metric_entity,
            )
            doc.hadMember(
                collection=operator_revision_entity,
                entity=metric_entity,
            )

        return doc

    def test_build(self):
        operator = Operator(
            id_=0,
            name=str(uuid.uuid4()),
        )
        operator_revision = OperatorRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            operator=operator,
            parameters=list(),
            parent_operator_revision_uuid=None,
        )
        metric = Metric(
            name=str(uuid.uuid4()),
            value=random.uniform(0, 1),
        )
        operator_run = OperatorRun(
            id_=str(uuid.uuid4()),
            created_at=today,
            metrics=[metric],
        )
        operator_execution = OperatorExecution(
            uuid=str(uuid.uuid4()),
            operator_revision=operator_revision,
            operator_run=operator_run,
            operator_step_type=OperatorStepType.ON_OP_EXECUTED,
            time=today,
        )
        model = OperationExecutionModel(
            operator_execution=operator_execution,
        )

        assert self.build(operator_execution) == model.build()
