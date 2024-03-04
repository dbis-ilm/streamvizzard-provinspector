import pytest

from provinspector.storage.adapter import start_docker_container, stop_docker_container


@pytest.fixture(scope="class")
def neo4j_context():
    docker_client, container = start_docker_container(
        docker_socket="unix:///run/user/1000/docker.sock",
        ports={
            "7687/tcp": ("127.0.0.1", 7687),
            "7474/tcp": ("127.0.0.1", 7474),
        },
        environment=["NEO4J_AUTH=neo4j/neo4jneo4j"],
        name="neo4j",
        image="neo4j:4.4",
    )

    if container is None:
        raise Exception("Error starting container")

    yield

    stop_docker_container(
        docker_client=docker_client,
        container=container,
    )
