from provinspector.domain.model import PipelineVersion
from provinspector.storage.repository import InMemoryRepository


class TestInMemoryRepository:
    def test_add_and_get(self):
        repo = InMemoryRepository()
        repo.clear()

        pv1 = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        pv2 = PipelineVersion(
            id_=1,
            parent_pipeline_version_id=pv1.id_,
        )

        repo.add(pv1)
        repo.add(pv2)

        assert repo.get(resource_type=PipelineVersion, id_=pv1.id_) == pv1
        assert repo.get(resource_type=PipelineVersion, id_=pv2.id_) == pv2

    def test_get_returns_none_if_repository_empty(self):
        repo = InMemoryRepository()
        repo.clear()

        assert repo.get(resource_type=PipelineVersion, id_=0) == None

    def test_list_all(self):
        repo = InMemoryRepository()
        repo.clear()

        pv1 = PipelineVersion(
            id_=0,
            parent_pipeline_version_id=None,
        )
        pv2 = PipelineVersion(
            id_=1,
            parent_pipeline_version_id=pv1.id_,
        )

        repo.add(pv1)
        repo.add(pv2)

        assert repo.list_all(resource_type=PipelineVersion, id_=pv1.id_) == [pv1]
        assert repo.list_all(resource_type=PipelineVersion, id_=pv2.id_) == [pv2]

        assert repo.list_all(resource_type=PipelineVersion) == [pv1, pv2]

    def test_list_all_returns_empty_list_if_repository_empty(self):
        repo = InMemoryRepository()
        repo.clear()

        assert repo.list_all(resource_type=PipelineVersion, id_=0) == []
