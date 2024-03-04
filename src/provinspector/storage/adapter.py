import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from docker import DockerClient
from docker.models.containers import Container
from py2neo import Graph


class DBMSType(Enum):
    NEO4J = "Neo4J"
    MEMGRAPH = "MemGraph"

    def __str__(self) -> str:
        return self.value


@dataclass
class Adapter:
    graph: Graph = field(init=False)
    database_name: str | None = None

    def connect(
        self,
        uri: str,
        auth: tuple[str, str],
        database_name: str,
        retries: int = 30,
    ) -> None:
        """
        Establish a connection to a Neo4J database.
        """

        for _ in range(retries):
            try:
                # Set driver and database
                self.graph = Graph(
                    uri=uri,
                    auth=auth,
                    name=database_name,
                )
                self.graph.run("MATCH () RETURN 1 LIMIT 1")
                break
            except Exception:
                time.sleep(1)

    def disconnect(self):
        """
        Disconnect from a Neo4J database.
        """

        if self.graph is None:
            return

        del self.graph

    def shutdown(self):
        raise NotImplementedError


def start_docker_container(
    docker_socket: str,
    ports: dict[str, Any],
    environment: list[str],
    name: str,
    image: str,
) -> tuple[DockerClient, Container]:
    # Initialize Docker client
    docker_client = DockerClient(base_url=docker_socket)

    # Run Docker container
    container: Container = docker_client.containers.run(
        remove=True,
        detach=True,
        ports=ports,
        environment=environment,
        name=name,
        image=image,
    )  # type:ignore

    return docker_client, container


def stop_docker_container(
    docker_client: DockerClient,
    container: Container,
):
    docker_client.close()
    container.stop()


@dataclass
class Neo4JAdapter(Adapter):
    use_docker: bool = True
    dbms_type: DBMSType = DBMSType.NEO4J
    docker_socket: str = "unix://var/run/docker.sock"
    docker_client: DockerClient = field(init=False)
    container: Container = field(init=False)

    def __post_init__(self):
        if self.use_docker:
            self.docker_client, self.container = start_docker_container(
                docker_socket=self.docker_socket,
                ports={
                    "7687/tcp": ("127.0.0.1", 7687),
                    "7474/tcp": ("127.0.0.1", 7474),
                },
                environment=["NEO4J_AUTH=neo4j/neo4jneo4j"],
                name="neo4j",
                image="neo4j:4.4",
            )

            if self.container is None:
                raise Exception("Error starting container")

        # Set default database name
        self.database_name = "neo4j"

        # Establish database connection
        self.connect(
            uri="bolt://127.0.0.1:7687",
            auth=("neo4j", "neo4jneo4j"),
            database_name=self.database_name,
        )

    def shutdown(self):
        # Remove database connection
        self.disconnect()

        # Close database driver and stop Docker container
        if self.use_docker:
            stop_docker_container(
                docker_client=self.docker_client,
                container=self.container,
            )


@dataclass
class MemgraphAdapter(Adapter):
    use_docker: bool = True
    dbms_type: DBMSType = DBMSType.MEMGRAPH
    docker_socket: str = "unix://var/run/docker.sock"
    docker_client: DockerClient = field(init=False)
    container: Container = field(init=False)

    def __post_init__(self):
        if self.use_docker:
            self.docker_client, self.container = start_docker_container(
                docker_socket=self.docker_socket,
                ports={
                    "7687/tcp": ("127.0.0.1", 7687),
                    "7444/tcp": ("127.0.0.1", 7444),
                    "3000/tcp": ("127.0.0.1", 3000),
                },
                environment=[],
                name="memgraph",
                image="memgraph/memgraph",
            )

            if self.container is None:
                raise Exception("Error starting container")

        # Set default database name
        self.database_name = "memgraph"

        # Establish database connection
        self.connect(
            uri="bolt://127.0.0.1:7687",
            auth=("", ""),
            database_name=self.database_name,
        )

    def shutdown(self):
        # Remove database connection
        self.disconnect()

        # Close database driver and stop Docker container
        if self.use_docker:
            stop_docker_container(
                docker_client=self.docker_client,
                container=self.container,
            )
