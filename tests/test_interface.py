import random
from uuid import uuid4

from provinspector.data import OperatorCreationPipelineChangeData


class TestOperatorCreationPipelineChangeData:
    def test_init(self):
        data = OperatorCreationPipelineChangeData(
            id=str(uuid4()),
            operator_id=random.randint(0, 10),
            operator_name="random-name",
            operator_data={},
        )
