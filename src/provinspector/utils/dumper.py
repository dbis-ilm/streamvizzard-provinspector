import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from provinspector.data import (
    ConnectionCreationPipelineChangeData,
    ConnectionDeletionPipelineChangeData,
    DebugStepData,
    MetricData,
    OperatorCreationPipelineChangeData,
    OperatorDeletionPipelineChangeData,
    OperatorModificationPipelineChangeData,
    PipelineChangeData,
)
from provinspector.domain.constants import OperatorStepType, PipelineChangeType


@dataclass
class JsonDumper:
    @staticmethod
    def metric_data_from_json(data: dict) -> MetricData:
        return MetricData(
            name=data["name"],
            value=data["value"],
        )

    @staticmethod
    def pipeline_change_data_from_json(data: dict) -> PipelineChangeData | None:
        change_type_name = data["updateType"]
        del data["updateType"]

        if (
            PipelineChangeType.from_string(change_type_name)
            == PipelineChangeType.OPERATOR_CREATION
        ):
            return OperatorCreationPipelineChangeData(
                id=data["uniqueID"],
                operator_id=data["opID"],
                operator_name=data["opName"],
                operator_data=data["opData"],
            )

        elif (
            PipelineChangeType.from_string(change_type_name)
            == PipelineChangeType.OPERATOR_MODIFICATION
        ):
            return OperatorModificationPipelineChangeData(
                id=data["uniqueID"],
                operator_id=data["opID"],
                operator_name=data["opName"],
                changed_parameter=data["changedParam"],
                changed_value=data["changedVal"],
            )

        elif (
            PipelineChangeType.from_string(change_type_name)
            == PipelineChangeType.OPERATOR_DELETION
        ):
            return OperatorDeletionPipelineChangeData(
                id=data["uniqueID"],
                operator_id=data["opID"],
                operator_name=data["opName"],
            )

        elif (
            PipelineChangeType.from_string(change_type_name)
            == PipelineChangeType.CONNECTION_CREATION
        ):
            return ConnectionCreationPipelineChangeData(
                id=data["uniqueID"],
                connection_id=data["conID"],
                from_operator_id=data["fromOpID"],
                to_operator_id=data["toOpID"],
                from_socket_id=data["fromSockID"],
                to_socket_id=data["toSockID"],
            )

        elif (
            PipelineChangeType.from_string(change_type_name)
            == PipelineChangeType.CONNECTION_DELETION
        ):
            return ConnectionDeletionPipelineChangeData(
                id=data["uniqueID"],
                connection_id=data["conID"],
                from_operator_id=data["fromOpID"],
                to_operator_id=data["toOpID"],
                from_socket_id=data["fromSockID"],
                to_socket_id=data["toSockID"],
            )

        return None

    @staticmethod
    def debug_step_data_from_json(raw: str) -> DebugStepData:
        data = json.loads(raw)

        # Fetch metrics
        metrics_data = data["metrics"]
        metrics = []
        for m in metrics_data:
            metrics.append(JsonDumper.metric_data_from_json(m))

        # Fetch pipeline changes
        changes_data = data["updates"]
        changes = None
        if changes_data is not None:
            changes = []
            for c in changes_data:
                changes.append(JsonDumper.pipeline_change_data_from_json(c))

        return DebugStepData(
            id=data["uniqueStepID"],
            timestamp=data["timeStamp"],
            branch_id=data["branchID"],
            branch_local_step_id=data["stepID"],
            parent_branch_id=data["parentBranchID"],
            operator_id=data["uniqueOpID"],
            operator_name=data["opName"],
            operator_step_type=OperatorStepType.from_string(data["stepType"]),
            operator_metrics=metrics,
            changes=changes,
        )

    @staticmethod
    def load_init_data(path: Path) -> Iterator[PipelineChangeData]:
        with open(path, "r") as file:
            for line in file.readlines():
                pipeline_change_data = JsonDumper.pipeline_change_data_from_json(
                    json.loads(line)
                )
                if pipeline_change_data is not None:
                    yield pipeline_change_data

    @staticmethod
    def load_execution_data(path: Path) -> Iterator[DebugStepData]:
        with open(path, "r") as file:
            for line in file.readlines():
                yield JsonDumper.debug_step_data_from_json(line)

    @staticmethod
    def pipeline_change_data_to_dict(
        data: PipelineChangeData,
    ) -> dict:  # type:ignore
        if data.change_type == PipelineChangeType.OPERATOR_CREATION and isinstance(
            data, OperatorCreationPipelineChangeData
        ):
            return {
                "uniqueID": data.id,
                "updateType": data.change_type.name,
                "opID": data.operator_id,
                "opName": data.operator_name,
                "opData": data.operator_data,
            }
        elif (
            data.change_type == PipelineChangeType.OPERATOR_MODIFICATION
            and isinstance(data, OperatorModificationPipelineChangeData)
        ):
            return {
                "uniqueID": data.id,
                "updateType": data.change_type.name,
                "opID": data.operator_id,
                "opName": data.operator_name,
                "changedParam": data.changed_parameter,
                "changedVal": data.changed_value,
            }
        elif data.change_type == PipelineChangeType.OPERATOR_DELETION and isinstance(
            data, OperatorDeletionPipelineChangeData
        ):
            return {
                "uniqueID": data.id,
                "updateType": data.change_type.name,
                "opID": data.operator_id,
                "opName": data.operator_name,
            }
        elif data.change_type == PipelineChangeType.CONNECTION_CREATION and isinstance(
            data, ConnectionCreationPipelineChangeData
        ):
            return {
                "uniqueID": data.id,
                "updateType": data.change_type.name,
                "conID": data.connection_id,
                "fromOpID": data.from_operator_id,
                "toOpID": data.to_operator_id,
                "fromSockID": data.from_socket_id,
                "toSockID": data.to_socket_id,
            }
        elif data.change_type == PipelineChangeType.CONNECTION_DELETION and isinstance(
            data, ConnectionDeletionPipelineChangeData
        ):
            return {
                "uniqueID": data.id,
                "updateType": data.change_type.name,
                "conID": data.connection_id,
                "fromOpID": data.from_operator_id,
                "toOpID": data.to_operator_id,
                "fromSockID": data.from_socket_id,
                "toSockID": data.to_socket_id,
            }

    @staticmethod
    def pipeline_change_data_to_json(data: dict[str, Any]) -> str:
        return json.dumps(data)

    @staticmethod
    def metric_data_to_dict(data: MetricData) -> dict:
        return {"name": data.name, "value": data.value}

    @staticmethod
    def debug_step_data_to_json(data: DebugStepData) -> str:
        return json.dumps(
            {
                "uniqueStepID": data.id,
                "timeStamp": data.timestamp,
                "branchID": data.branch_id,
                "parentBranchID": data.parent_branch_id,
                "stepID": data.branch_local_step_id,
                "uniqueOpID": data.operator_id,
                "opName": data.operator_name,
                "stepType": data.operator_step_type.name,
                "metrics": (
                    [JsonDumper.metric_data_to_dict(m) for m in data.operator_metrics]
                    if data.operator_metrics is not None
                    else []
                ),
                "updates": (
                    [JsonDumper.pipeline_change_data_to_dict(c) for c in data.changes]
                    if data.changes is not None
                    else None
                ),
            }
        )
