from provinspector.storage.adapter import DBMSType, MemgraphAdapter, Neo4JAdapter


class TestDBMSTypeEnum:
    def test(self):
        assert str(DBMSType.NEO4J) == "Neo4J"
        assert str(DBMSType.MEMGRAPH) == "MemGraph"


class TestNeo4JAdapter:
    def test_post_init(self):
        adapter = Neo4JAdapter(
            docker_socket="unix:///run/user/1000/docker.sock",
        )
        adapter.shutdown()


class TestMemgraphAdapter:
    def test_post_init(self):
        adapter = MemgraphAdapter(
            docker_socket="unix:///run/user/1000/docker.sock",
        )
        adapter.shutdown()
