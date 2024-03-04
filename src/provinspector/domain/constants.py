from enum import Enum

import prov.constants


class PipelineChangeType(Enum):
    OPERATOR_CREATION = "OperatorCreation"
    OPERATOR_MODIFICATION = "OperatorModification"
    OPERATOR_DELETION = "OperatorDeletion"
    CONNECTION_CREATION = "ConnectionCreation"
    CONNECTION_DELETION = "ConnectionDeletion"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(pipeline_change_type_str: str) -> "PipelineChangeType":
        if pipeline_change_type_str in (
            "OPERATOR_CREATION",
            "OperatorCreation",
        ):
            return PipelineChangeType.OPERATOR_CREATION
        elif pipeline_change_type_str in (
            "OPERATOR_MODIFICATION",
            "OperatorModification",
        ):
            return PipelineChangeType.OPERATOR_MODIFICATION
        elif pipeline_change_type_str in (
            "OPERATOR_DELETION",
            "OperatorDeletion",
        ):
            return PipelineChangeType.OPERATOR_DELETION
        elif pipeline_change_type_str in (
            "CONNECTION_CREATION",
            "ConnectionCreation",
        ):
            return PipelineChangeType.CONNECTION_CREATION
        elif pipeline_change_type_str in (
            "CONNECTION_DELETION",
            "ConnectionDeletion",
        ):
            return PipelineChangeType.CONNECTION_DELETION
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(pipeline_change_type: "PipelineChangeType") -> str:
        return str(pipeline_change_type)


class OperatorStepType(Enum):
    ON_SOURCE_PRODUCED_TUPLE = "OnSourceProducedTuple"
    ON_TUPLE_TRANSMITTED = "OnTupleTransmitted"
    ON_STREAM_PROCESS_TUPLE = "OnStreamProcessTuple"
    PRE_TUPLE_PROCESSED = "PreTupleProcessed"
    ON_TUPLE_PROCESSED = "OnTupleProcessed"
    ON_OP_EXECUTED = "OnOpExecuted"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def from_string(format_str: str) -> "OperatorStepType":
        if format_str in (
            "ON_SOURCE_PRODUCED_TUPLE",
            "OnSourceProducedTuple",
        ):
            return OperatorStepType.ON_SOURCE_PRODUCED_TUPLE
        elif format_str in (
            "ON_TUPLE_TRANSMITTED",
            "OnTupleTransmitted",
        ):
            return OperatorStepType.ON_TUPLE_TRANSMITTED
        elif format_str in (
            "ON_STREAM_PROCESS_TUPLE",
            "OnStreamProcessTuple",
        ):
            return OperatorStepType.ON_STREAM_PROCESS_TUPLE
        elif format_str in (
            "PRE_TUPLE_PROCESSED",
            "PreTupleProcessed",
        ):
            return OperatorStepType.PRE_TUPLE_PROCESSED
        elif format_str in (
            "ON_TUPLE_PROCESSED",
            "OnTupleProcessed",
        ):
            return OperatorStepType.ON_TUPLE_PROCESSED
        elif format_str in (
            "ON_OP_EXECUTED",
            "OnOpExecuted",
        ):
            return OperatorStepType.ON_OP_EXECUTED
        else:
            raise NotImplementedError

    @staticmethod
    def to_string(pipeline_change_type: "OperatorStepType") -> str:
        return str(pipeline_change_type)


class ProvRole:
    CREATED_PIPELINE_VERSION = "CreatedPipelineVersion"
    CREATED_PIPELINE_VERSION_REVISION = "CreatedPipelineVersionRevision"

    CREATED_OPERATOR = "CreatedOperator"
    MODIFIED_OPERATOR = "ModifiedOperator"
    DELETED_OPERATOR = "DeletedOperator"

    CREATED_CONNECTION = "CreatedConnection"
    DELETED_CONNECTION = "DeletedConnection"

    CREATED_OPERATOR_RUN = "AddedOperatorRun"

    USED_PARENT_PIPELINE_VERSION = "UsedParentPipelineVersion"
    USED_PARENT_PIPELINE_VERSION_REVISION = "UsedParentPipelineVersionRevision"
    USED_OPERATOR_REVISION = "UsedOperatorRevision"
    USED_PARENT_OPERATOR_REVISION = "UsedParentOperatorRevision"


class ProvType:
    PIPELINE_VERSION = "PipelineVersion"
    PIPELINE_VERSION_REVISION = "PipelineVersionRevision"
    OPERATOR = "Operator"
    OPERATOR_REVISION = "OperatorRevision"
    PARAMETER = "Parameter"
    OPERATOR_RUN = "OperatorRun"
    METRIC = "Metric"
    CONNECTION = "Connection"

    PIPELINE_VERSION_CREATION = "PipelineVersionCreation"
    PIPELINE_CHANGE = "PipelineChange"
    OPERATOR_EXECUTION = "OperatorExecution"

    COLLECTION = prov.constants.PROV_ATTR_COLLECTION
