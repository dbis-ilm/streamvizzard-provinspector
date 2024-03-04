import datetime
import random
import uuid
from pathlib import Path

import pytest
from py2neo.cypher import Cursor

from provinspector.domain.model import (
    Connection,
    Operator,
    OperatorRevision,
    Parameter,
    PipelineVersion,
    PipelineVersionCreation,
    PipelineVersionRevision,
)
from provinspector.prov.model import Model, PipelineVersionCreationModel
from provinspector.provinspector import ProvInspector
from provinspector.storage.adapter import Neo4JAdapter
from provinspector.storage.database import ProvGraphDatabase
from provinspector.utils.dumper import JsonDumper
from tests.fixtures import neo4j_context


def query_and_print_all_nodes(
    provinspector: ProvInspector,
    do_print: bool = True,
) -> None:
    result: Cursor = provinspector.query(query="""MATCH (n) RETURN (n)""")
    if do_print:
        for r in result:
            print(r)


def query_and_print_all_relationships(
    provinspector: ProvInspector,
    do_print: bool = True,
) -> None:
    result: Cursor = provinspector.query(
        query="""
        MATCH ()-[r]-()
        RETURN r
        """
    )
    if do_print:
        for r in result:
            print(r)


@pytest.mark.usefixtures("neo4j_context")
class TestProvInspector:
    def test_provinspector(self):
        # Initialize ProvInspector
        provinspector = ProvInspector(
            provenance_database=ProvGraphDatabase(
                adapter=Neo4JAdapter(
                    use_docker=False,
                )
            ),
        )

        # Create PipelineVersionCreation
        today = datetime.datetime.now()
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
        pipeline_version_revision = PipelineVersionRevision(
            uuid=str(uuid.uuid4()),
            id_=0,
            pipeline_version=pipeline_version,
            parent_pipeline_version_revision_uuid=None,
            operators=[operator_revision],
            connections=[connection],
        )
        pipeline_version_creation = PipelineVersionCreation(
            uuid=str(uuid.uuid4()),
            pipeline_version_revision=pipeline_version_revision,
            parent_pipeline_version_creation_uuid=None,
            time=today,
        )

        # Create PipelineVersionCreationModel
        model: Model = PipelineVersionCreationModel(
            pipeline_version_creation=pipeline_version_creation,
            parent_pipeline_version_revision=None,
            parent_pipeline_version_creation=None,
        )

        # Push update
        provinspector.add_model(
            model=model,
        )

        # Query
        query_and_print_all_nodes(provinspector=provinspector)
        query_and_print_all_relationships(provinspector=provinspector)

        # Clear
        provinspector.clear()

        # Query
        query_and_print_all_nodes(provinspector=provinspector)

        # Shutdown
        provinspector.shutdown()

    def test_initialize_and_update(self):
        # Initialize ProvInspector
        provinspector = ProvInspector(
            provenance_database=ProvGraphDatabase(
                adapter=Neo4JAdapter(
                    use_docker=False,
                )
            ),
        )
        provinspector.clear()

        # Test initialize
        data = list(
            JsonDumper.load_init_data(
                path=Path(__file__).parent / "resources" / "dump_init.txt"
            )
        )
        provinspector.initialize(data=data)

        # Test update
        data = list(
            JsonDumper.load_execution_data(
                path=Path(__file__).parent / "resources" / "dump_exec.txt"
            )
        )
        for d in data:
            provinspector.update(data=d)

        # Query and print results
        query_and_print_all_nodes(
            provinspector=provinspector,
            do_print=False,
        )
        query_and_print_all_relationships(
            provinspector=provinspector,
            do_print=False,
        )

        # Shutdown
        provinspector.shutdown()
