from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4
from warnings import warn

from py2neo.database import Cursor

from provinspector.data import (
    ConnectionCreationPipelineChangeData,
    ConnectionDeletionPipelineChangeData,
    DebugStepData,
    OperatorCreationPipelineChangeData,
    OperatorDeletionPipelineChangeData,
    OperatorModificationPipelineChangeData,
    PipelineChangeData,
)
from provinspector.domain.constants import PipelineChangeType
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
    ConnectionCreationModel,
    ConnectionDeletionModel,
    Model,
    OperationExecutionModel,
    OperatorCreationModel,
    OperatorDeletionModel,
    OperatorModificationModel,
    PipelineVersionCreationModel,
)
from provinspector.storage.database import ProvGraphDatabase
from provinspector.storage.repository import InMemoryRepository
from provinspector.utils.log import create_logger

# Initial ids
INITIAL_PIPELINE_VERSION_ID: int = 0
INITIAL_PIPELINE_VERSION_REVISION_ID: int = 0
INITIAL_OPERATOR_REVISION_ID: int = 0
INITIAL_PIPELINE_VERSION_CREATION_ID = str(uuid4())
INITIAL_TIME: float = 0.0
INITIAL_DATETIME = datetime.fromtimestamp(INITIAL_TIME)


@dataclass
class ProvInspector:
    provenance_database: ProvGraphDatabase = field(default_factory=ProvGraphDatabase)
    object_repository: InMemoryRepository = field(default_factory=InMemoryRepository)
    initialized: bool = False
    logging: bool = False

    last_pipeline_version_id: int = INITIAL_PIPELINE_VERSION_ID
    last_pipeline_version_revision_id: int = INITIAL_PIPELINE_VERSION_REVISION_ID

    def __post_init__(self):
        if self.logging:
            create_logger()

    def shutdown(self) -> None:
        self.provenance_database.shutdown()

    def clear(self) -> None:
        """
        Reset ProvInspector for StreamVizzard Debugger restart.
        """

        # Clear object repository and database
        self.object_repository.clear()
        self.provenance_database.clear()

        # Set initialized flag
        self.initialized = False

    def add_model(
        self,
        model: Model,
    ) -> None:
        """
        Build provenance submodel graph and update provenance graph.
        """

        graph = model.build()
        self.provenance_database.import_graph(graph=graph)

    def initialize(
        self,
        data: list[PipelineChangeData],
    ) -> None:
        """
        Initialize provenance graph from initial pipeline change data.
        """

        if not self.initialized:
            # Create PipelineVersionCreation
            operators = []
            connections = []

            for pipeline_change_data in data:
                if isinstance(pipeline_change_data, OperatorCreationPipelineChangeData):
                    operator = Operator(
                        id_=pipeline_change_data.operator_id,
                        name=pipeline_change_data.operator_name,
                    )
                    parameters = [
                        Parameter(
                            name=key,
                            value=value,
                        )
                        for key, value in pipeline_change_data.operator_data.items()
                    ]
                    operator_revision = OperatorRevision(
                        uuid=str(uuid4()),
                        id_=0,
                        operator=operator,
                        parameters=parameters,
                        parent_operator_revision_uuid=None,
                    )
                    operators.append(operator_revision)

                elif isinstance(
                    pipeline_change_data, ConnectionCreationPipelineChangeData
                ):
                    connection = Connection(
                        id_=pipeline_change_data.connection_id,
                        from_operator_id=pipeline_change_data.from_operator_id,
                        to_operator_id=pipeline_change_data.to_operator_id,
                    )
                    connections.append(connection)

            pipeline_version = PipelineVersion(
                id_=INITIAL_PIPELINE_VERSION_ID,
                parent_pipeline_version_id=None,
            )

            pipeline_version_revision = PipelineVersionRevision(
                uuid=str(uuid4()),
                id_=INITIAL_PIPELINE_VERSION_REVISION_ID,
                pipeline_version=pipeline_version,
                parent_pipeline_version_revision_uuid=None,
                operators=operators,
                connections=connections,
            )

            pipeline_version_creation = PipelineVersionCreation(
                uuid=INITIAL_PIPELINE_VERSION_CREATION_ID,
                pipeline_version_revision=pipeline_version_revision,
                parent_pipeline_version_creation_uuid=None,
                time=INITIAL_DATETIME,
            )

            # Add objects to object repository
            self.object_repository.add(pipeline_version)
            self.object_repository.add(pipeline_version_revision)
            self.object_repository.add(pipeline_version_creation)

            # Add PipelineVersionCreationModel
            self.add_model(
                model=PipelineVersionCreationModel(
                    pipeline_version_creation=pipeline_version_creation,
                    parent_pipeline_version_revision=None,
                    parent_pipeline_version_creation=None,
                ),
            )

            # Set initialized flag
            self.initialized = True

        else:
            warn("Already initialized")

    def update(
        self,
        data: DebugStepData,
    ) -> None:
        """
        Update provenance graph from debug step data.
        """

        pipeline_version: PipelineVersion
        parent_pipeline_version_revision: PipelineVersionRevision
        debug_step_datetime: datetime = datetime.fromtimestamp(data.timestamp)

        # If not initialized and no pipeline version exists:
        if (
            not self.initialized
            and self.object_repository.list_all(resource_type=PipelineVersion) == []
        ):
            # Create initial pipeline version and initial pipeline version revision
            pipeline_version = PipelineVersion(
                id_=0,
                parent_pipeline_version_id=None,
            )
            parent_pipeline_version_revision = PipelineVersionRevision(
                uuid=str(uuid4()),
                id_=0,
                pipeline_version=pipeline_version,
                parent_pipeline_version_revision_uuid=None,
                operators=[],  # if not initialized via `initialize`, empty for initial version revision
                connections=[],  # if not initialized via `initialize`, empty for initial version revision
            )
            pipeline_version_creation = PipelineVersionCreation(
                uuid=str(uuid4()),
                pipeline_version_revision=parent_pipeline_version_revision,
                parent_pipeline_version_creation_uuid=None,
                time=debug_step_datetime,
            )

            # Add objects to object repository
            self.object_repository.add(pipeline_version)
            self.object_repository.add(parent_pipeline_version_revision)
            self.object_repository.add(pipeline_version_creation)

            # Add model
            pipeline_version_creation_model = PipelineVersionCreationModel(
                pipeline_version_creation=pipeline_version_creation,
                parent_pipeline_version_revision=None,
                parent_pipeline_version_creation=None,
            )
            self.add_model(model=pipeline_version_creation_model)

        # Should be already initialized
        else:
            pipeline_version = self.object_repository.get(
                resource_type=PipelineVersion,
                id_=data.branch_id,
            )  # type:ignore

            # If pipeline version already exists
            if pipeline_version:
                # If current pipeline version is also last used pipeline version, fetch revision directly by id
                if self.last_pipeline_version_id == data.branch_id:
                    parent_pipeline_version_revision = self.object_repository.get(
                        resource_type=PipelineVersionRevision,
                        pipeline_version=pipeline_version,
                        id_=self.last_pipeline_version_revision_id,
                    )  # type:ignore
                # Else, search last revision
                else:
                    parent_pipeline_version_revision = self.object_repository.list_all(
                        resource_type=PipelineVersionRevision,
                        pipeline_version=pipeline_version,
                    )[-1]

            # Else, create new pipeline version and initial revision of that pipeline version (pipeline version creation model)
            else:
                parent_pipeline_version = self.object_repository.get(
                    resource_type=PipelineVersion,
                    id_=data.parent_branch_id,  # should not be None here
                )
                pipeline_version = PipelineVersion(
                    id_=data.branch_id,
                    parent_pipeline_version_id=parent_pipeline_version.id_,  # type:ignore
                )
                parent_parent_pipeline_version_revision: PipelineVersionRevision = (
                    self.object_repository.list_all(
                        resource_type=PipelineVersionRevision,
                        pipeline_version=parent_pipeline_version,
                    )[-1]
                )
                parent_pipeline_version_revision = PipelineVersionRevision(
                    uuid=str(uuid4()),
                    id_=0,
                    pipeline_version=pipeline_version,
                    parent_pipeline_version_revision_uuid=parent_parent_pipeline_version_revision.uuid,
                    operators=parent_parent_pipeline_version_revision.operators,
                    connections=parent_parent_pipeline_version_revision.connections,
                )  # initial pipeline version revision, parent of potential succeeding pipeline version revisions (see below)
                parent_parent_pipeline_version_creation = self.object_repository.get(
                    resource_type=PipelineVersionCreation,
                    pipeline_version_revision=parent_pipeline_version_revision,
                )
                pipeline_version_creation: PipelineVersionCreation = (
                    PipelineVersionCreation(
                        uuid=str(uuid4()),
                        pipeline_version_revision=parent_pipeline_version_revision,
                        parent_pipeline_version_creation_uuid=(
                            parent_parent_pipeline_version_creation.uuid
                            if parent_parent_pipeline_version_creation
                            else None
                        ),
                        time=debug_step_datetime,
                    )
                )

                # Add objects to object repository
                self.object_repository.add(pipeline_version)
                self.object_repository.add(parent_pipeline_version_revision)
                self.object_repository.add(pipeline_version_creation)

                # Add model
                pipeline_version_creation_model = PipelineVersionCreationModel(
                    pipeline_version_creation=pipeline_version_creation,
                    parent_pipeline_version_revision=parent_parent_pipeline_version_revision,
                    parent_pipeline_version_creation=(
                        parent_parent_pipeline_version_creation
                        if parent_parent_pipeline_version_creation
                        else None
                    ),
                )
                self.add_model(model=pipeline_version_creation_model)

        # Set last pipeline version id and last pipeline version revision id
        self.last_pipeline_version_id = pipeline_version.id_
        self.last_pipeline_version_revision_id = parent_pipeline_version_revision.id_

        # Create new pipeline version revisions from pipeline changes
        if data.changes is not None:
            for c in data.changes:
                if (
                    c.change_type == PipelineChangeType.OPERATOR_CREATION
                    and isinstance(c, OperatorCreationPipelineChangeData)
                ):
                    operator = Operator(
                        id_=c.operator_id,
                        name=c.operator_name,
                    )
                    operator_revision_uuid = str(uuid4())
                    parameters = [
                        Parameter(
                            name=key,
                            value=value,
                        )
                        for key, value in c.operator_data.items()
                    ]
                    operator_revision = OperatorRevision(
                        uuid=operator_revision_uuid,
                        id_=0,
                        operator=operator,
                        parameters=parameters,
                        parent_operator_revision_uuid=None,
                    )
                    operators = parent_pipeline_version_revision.operators
                    operators.append(operator_revision)
                    pipeline_version_revision = PipelineVersionRevision(
                        uuid=str(uuid4()),
                        id_=parent_pipeline_version_revision.id_ + 1,
                        pipeline_version=parent_pipeline_version_revision.pipeline_version,
                        parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
                        operators=operators,
                        connections=parent_pipeline_version_revision.connections,
                    )
                    parent_pipeline_change_list = self.object_repository.list_all(
                        resource_type=PipelineChange,
                        pipeline_version_revision=parent_pipeline_version_revision,
                    )
                    parent_pipeline_change = None
                    if parent_pipeline_change_list != []:
                        parent_pipeline_change = parent_pipeline_change_list[-1]

                    pipeline_change = OperatorCreationPipelineChange(
                        uuid=str(uuid4()),
                        time=debug_step_datetime,
                        operator_revision=operator_revision,
                        pipeline_version_revision=pipeline_version_revision,
                        parent_pipeline_change_uuid=(
                            parent_pipeline_change.uuid
                            if parent_pipeline_change
                            else None
                        ),
                    )

                    model = OperatorCreationModel(
                        pipeline_change=pipeline_change,
                        parent_pipeline_change=(
                            parent_pipeline_change if parent_pipeline_change else None
                        ),
                        parent_pipeline_version_revision=parent_pipeline_version_revision,
                    )

                    self.add_model(model=model)

                elif (
                    c.change_type == PipelineChangeType.OPERATOR_MODIFICATION
                    and isinstance(c, OperatorModificationPipelineChangeData)
                ):
                    operator = Operator(
                        id_=c.operator_id,
                        name=c.operator_name,
                    )
                    parent_operator_revision: OperatorRevision = next(
                        oprev
                        for oprev in parent_pipeline_version_revision.operators
                        if oprev.operator == operator
                    )
                    parameters: list[Parameter] = [
                        Parameter(
                            name=parameter.name,
                            value=parameter.value,
                        )
                        for parameter in parent_operator_revision.parameters
                        if parameter.name != c.changed_parameter
                    ]
                    parameters.append(
                        Parameter(
                            name=c.changed_parameter,
                            value=c.changed_value,
                        )
                    )  # type:ignore
                    operator_revision = OperatorRevision(
                        uuid=str(uuid4()),
                        id_=parent_operator_revision.id_ + 1,
                        operator=operator,
                        parameters=parameters,
                        parent_operator_revision_uuid=parent_operator_revision.uuid,
                    )
                    operators = parent_pipeline_version_revision.operators
                    operators.append(operator_revision)
                    pipeline_version_revision = PipelineVersionRevision(
                        uuid=str(uuid4()),
                        id_=parent_pipeline_version_revision.id_ + 1,
                        pipeline_version=parent_pipeline_version_revision.pipeline_version,
                        parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
                        operators=operators,
                        connections=parent_pipeline_version_revision.connections,
                    )
                    parent_pipeline_change_list = self.object_repository.list_all(
                        resource_type=PipelineChange,
                        pipeline_version_revision=parent_pipeline_version_revision,
                    )
                    parent_pipeline_change = None
                    if parent_pipeline_change_list != []:
                        parent_pipeline_change = parent_pipeline_change_list[-1]

                    pipeline_change = OperatorModificationPipelineChange(
                        uuid=str(uuid4()),
                        time=debug_step_datetime,
                        operator_revision=operator_revision,
                        pipeline_version_revision=pipeline_version_revision,
                        parent_pipeline_change_uuid=(
                            parent_pipeline_change.uuid
                            if parent_pipeline_change
                            else None
                        ),
                    )

                    model = OperatorModificationModel(
                        pipeline_change=pipeline_change,
                        parent_pipeline_change=(
                            parent_pipeline_change if parent_pipeline_change else None
                        ),
                        parent_operator_revision=parent_operator_revision,
                        parent_pipeline_version_revision=parent_pipeline_version_revision,
                    )

                    self.add_model(model=model)

                elif (
                    c.change_type == PipelineChangeType.OPERATOR_DELETION
                    and isinstance(c, OperatorDeletionPipelineChangeData)
                ):
                    operator = Operator(
                        id_=c.operator_id,
                        name=c.operator_name,
                    )
                    operator_revision: OperatorRevision = next(
                        oprev
                        for oprev in parent_pipeline_version_revision.operators
                        if oprev.operator == operator
                    )
                    operators: list[OperatorRevision] = (
                        parent_pipeline_version_revision.operators
                    )
                    operators.remove(operator_revision)
                    pipeline_version_revision = PipelineVersionRevision(
                        uuid=str(uuid4()),
                        id_=parent_pipeline_version_revision.id_ + 1,
                        pipeline_version=parent_pipeline_version_revision.pipeline_version,
                        parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
                        operators=operators,
                        connections=parent_pipeline_version_revision.connections,
                    )
                    parent_pipeline_change_list = self.object_repository.list_all(
                        resource_type=PipelineChange,
                        pipeline_version_revision=parent_pipeline_version_revision,
                    )
                    parent_pipeline_change = None
                    if parent_pipeline_change_list != []:
                        parent_pipeline_change = parent_pipeline_change_list[-1]
                    pipeline_change = OperatorDeletionPipelineChange(
                        uuid=str(uuid4()),
                        time=debug_step_datetime,
                        operator_revision=operator_revision,
                        pipeline_version_revision=pipeline_version_revision,
                        parent_pipeline_change_uuid=(
                            parent_pipeline_change.uuid
                            if parent_pipeline_change
                            else None
                        ),
                    )

                    model = OperatorDeletionModel(
                        pipeline_change=pipeline_change,
                        parent_pipeline_change=(
                            parent_pipeline_change if parent_pipeline_change else None
                        ),
                        parent_pipeline_version_revision=parent_pipeline_version_revision,
                    )

                    self.add_model(model=model)

                elif (
                    c.change_type == PipelineChangeType.CONNECTION_CREATION
                    and isinstance(c, ConnectionCreationPipelineChangeData)
                ):
                    connection = Connection(
                        id_=c.connection_id,
                        from_operator_id=c.from_operator_id,
                        to_operator_id=c.to_operator_id,
                    )
                    connections: list[Connection] = (
                        parent_pipeline_version_revision.connections
                    )
                    connections.append(connection)
                    pipeline_version_revision = PipelineVersionRevision(
                        uuid=str(uuid4()),
                        id_=parent_pipeline_version_revision.id_ + 1,
                        pipeline_version=parent_pipeline_version_revision.pipeline_version,
                        parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
                        operators=parent_pipeline_version_revision.operators,
                        connections=connections,
                    )
                    parent_pipeline_change_list = self.object_repository.list_all(
                        resource_type=PipelineChange,
                        pipeline_version_revision=parent_pipeline_version_revision,
                    )
                    parent_pipeline_change = None
                    if parent_pipeline_change_list != []:
                        parent_pipeline_change = parent_pipeline_change_list[-1]
                    pipeline_change = ConnectionCreationPipelineChange(
                        uuid=str(uuid4()),
                        time=debug_step_datetime,
                        connection=connection,
                        pipeline_version_revision=pipeline_version_revision,
                        parent_pipeline_change_uuid=(
                            parent_pipeline_change.uuid
                            if parent_pipeline_change
                            else None
                        ),
                    )

                    model = ConnectionCreationModel(
                        pipeline_change=pipeline_change,
                        parent_pipeline_change=(
                            parent_pipeline_change if parent_pipeline_change else None
                        ),
                        parent_pipeline_version_revision=parent_pipeline_version_revision,
                    )

                    self.add_model(model=model)

                elif (
                    c.change_type == PipelineChangeType.CONNECTION_DELETION
                    and isinstance(c, ConnectionDeletionPipelineChangeData)
                ):
                    connection = Connection(
                        id_=c.connection_id,
                        from_operator_id=c.from_operator_id,
                        to_operator_id=c.to_operator_id,
                    )
                    connections: list[Connection] = (
                        parent_pipeline_version_revision.connections
                    )
                    connections.append(connection)
                    pipeline_version_revision = PipelineVersionRevision(
                        uuid=str(uuid4()),
                        id_=parent_pipeline_version_revision.id_ + 1,
                        pipeline_version=parent_pipeline_version_revision.pipeline_version,
                        parent_pipeline_version_revision_uuid=parent_pipeline_version_revision.uuid,
                        operators=parent_pipeline_version_revision.operators,
                        connections=connections,
                    )
                    parent_pipeline_change_list = self.object_repository.list_all(
                        resource_type=PipelineChange,
                        pipeline_version_revision=parent_pipeline_version_revision,
                    )
                    parent_pipeline_change = None
                    if parent_pipeline_change_list != []:
                        parent_pipeline_change = parent_pipeline_change_list[-1]
                    pipeline_change = ConnectionDeletionPipelineChange(
                        uuid=str(uuid4()),
                        time=debug_step_datetime,
                        connection=connection,
                        pipeline_version_revision=pipeline_version_revision,
                        parent_pipeline_change_uuid=(
                            parent_pipeline_change.uuid
                            if parent_pipeline_change
                            else None
                        ),
                    )
                    model = ConnectionDeletionModel(
                        pipeline_change=pipeline_change,
                        parent_pipeline_change=(
                            parent_pipeline_change if parent_pipeline_change else None
                        ),
                        parent_pipeline_version_revision=parent_pipeline_version_revision,
                    )

                    self.add_model(model=model)

        # Create new operator run from operator execution
        if data.operator_metrics is not None:
            operator = Operator(
                id_=data.operator_id,
                name=data.operator_name,
            )
            operator_revision: OperatorRevision = next(
                oprev
                for oprev in parent_pipeline_version_revision.operators
                if oprev.operator == operator
            )
            metrics: list[Metric] = []
            for m in data.operator_metrics:
                metrics.append(
                    Metric(
                        name=m.name,
                        value=m.value,
                    )
                )

            operator_run = OperatorRun(
                id_=str(uuid4()),
                created_at=datetime.fromtimestamp(data.timestamp),
                metrics=metrics,
            )

            operator_execution = OperatorExecution(
                uuid=str(uuid4()),
                operator_revision=operator_revision,
                operator_run=operator_run,
                operator_step_type=data.operator_step_type,
                time=debug_step_datetime,
            )

            operator_execution_model = OperationExecutionModel(
                operator_execution=operator_execution,
            )

            self.add_model(model=operator_execution_model)

    def query(
        self,
        query: str,
    ) -> Cursor:
        """
        Query provenance graph.
        """

        return self.provenance_database.adapter.graph.run(
            cypher=query,
        )
