import pathlib
from collections import defaultdict
from typing import NamedTuple, Type

import prov.dot
import prov.model

OUTPUT_PDF = False
OUTPUT_SVG = True
OUTPUT_DOT = False
BG_TRANSPARENT = False


def create_pipeline_version_creation_model() -> prov.model.ProvDocument:
    model = prov.model.ProvDocument()
    model.set_default_namespace(uri="provinspector:")

    model.activity("PipelineVersionCreation")
    model.activity("ParentPipelineVersionCreation")
    model.wasInformedBy(
        informed="PipelineVersionCreation",
        informant="ParentPipelineVersionCreation",
    )
    model.entity("PipelineVersionRevision")
    model.entity("OperatorRevision")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="OperatorRevision",
    )
    model.entity("Connection")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="Connection",
    )
    model.wasGeneratedBy(
        entity="PipelineVersionRevision",
        activity="PipelineVersionCreation",
    )
    model.entity("ParentPipelineVersionRevision")
    model.wasDerivedFrom(
        generatedEntity="PipelineVersionRevision",
        usedEntity="ParentPipelineVersionRevision",
    )
    model.used(
        activity="PipelineVersionCreation",
        entity="ParentPipelineVersionRevision",
    )
    model.hadMember(
        collection="ParentPipelineVersionRevision",
        entity="OperatorRevision",
    )
    model.hadMember(
        collection="ParentPipelineVersionRevision",
        entity="Connection",
    )
    model.entity("PipelineVersion")
    model.specializationOf(
        specificEntity="PipelineVersionRevision",
        generalEntity="PipelineVersion",
    )
    model.wasGeneratedBy(
        entity="PipelineVersion",
        activity="PipelineVersionCreation",
    )
    model.entity("ParentPipelineVersion")
    model.wasGeneratedBy(
        entity="ParentPipelineVersion",
        activity="ParentPipelineVersionCreation",
    )
    model.specializationOf(
        specificEntity="ParentPipelineVersionRevision",
        generalEntity="ParentPipelineVersion",
    )
    model.wasDerivedFrom(
        generatedEntity="PipelineVersion",
        usedEntity="ParentPipelineVersion",
    )
    model.used(
        activity="PipelineVersionCreation",
        entity="ParentPipelineVersion",
    )

    return model


def create_operator_creation_model() -> prov.model.ProvDocument:
    model = prov.model.ProvDocument()
    model.set_default_namespace(uri="provinspector:")

    model.activity("PipelineChange")
    model.activity("ParentPipelineChange")
    model.wasInformedBy(
        informed="PipelineChange",
        informant="ParentPipelineChange",
    )
    model.entity("OperatorRevision")
    model.wasGeneratedBy(
        entity="OperatorRevision",
        activity="PipelineChange",
    )
    model.entity("Operator")
    model.specializationOf(
        specificEntity="OperatorRevision",
        generalEntity="Operator",
    )
    model.entity("Parameter")
    model.hadMember(
        collection="OperatorRevision",
        entity="Parameter",
    )
    model.entity("PipelineVersionRevision")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="OperatorRevision",
    )
    model.entity("Connection")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="Connection",
    )
    model.wasGeneratedBy(
        entity="PipelineVersionRevision",
        activity="PipelineChange",
    )
    model.entity("PipelineVersion")
    model.specializationOf(
        specificEntity="PipelineVersionRevision",
        generalEntity="PipelineVersion",
    )
    model.entity("ParentPipelineVersionRevision")
    model.wasRevisionOf(
        generatedEntity="PipelineVersionRevision",
        usedEntity="ParentPipelineVersionRevision",
    )
    model.used(
        activity="PipelineChange",
        entity="ParentPipelineVersionRevision",
    )

    return model


def create_operator_modification_model() -> prov.model.ProvDocument:
    model = prov.model.ProvDocument()
    model.set_default_namespace(uri="provinspector:")

    model.activity("PipelineChange")
    model.activity("ParentPipelineChange")
    model.wasInformedBy(
        informed="PipelineChange",
        informant="ParentPipelineChange",
    )
    model.entity("OperatorRevision")
    model.wasGeneratedBy(
        entity="OperatorRevision",
        activity="PipelineChange",
    )
    model.entity("ParentOperatorRevision")
    model.wasRevisionOf(
        generatedEntity="OperatorRevision",
        usedEntity="ParentOperatorRevision",
    )
    model.used(
        activity="PipelineChange",
        entity="ParentOperatorRevision",
    )
    model.entity("Operator")
    model.specializationOf(
        specificEntity="OperatorRevision",
        generalEntity="Operator",
    )
    model.entity("Parameter")
    model.hadMember(
        collection="OperatorRevision",
        entity="Parameter",
    )
    model.entity("PipelineVersionRevision")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="OperatorRevision",
    )
    model.entity("Connection")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="Connection",
    )
    model.wasGeneratedBy(
        entity="PipelineVersionRevision",
        activity="PipelineChange",
    )
    model.entity("PipelineVersion")
    model.specializationOf(
        specificEntity="PipelineVersionRevision",
        generalEntity="PipelineVersion",
    )
    model.entity("ParentPipelineVersionRevision")
    model.wasRevisionOf(
        generatedEntity="PipelineVersionRevision",
        usedEntity="ParentPipelineVersionRevision",
    )
    model.used(
        activity="PipelineChange",
        entity="ParentPipelineVersionRevision",
    )

    return model


def create_operator_deletion_model() -> prov.model.ProvDocument:
    model = prov.model.ProvDocument()
    model.set_default_namespace(uri="provinspector:")

    model.activity("PipelineChange")
    model.activity("ParentPipelineChange")
    model.wasInformedBy(
        informed="PipelineChange",
        informant="ParentPipelineChange",
    )
    model.entity("OperatorRevision")
    model.wasInvalidatedBy(
        entity="OperatorRevision",
        activity="PipelineChange",
    )
    model.entity("Operator")
    model.specializationOf(
        specificEntity="OperatorRevision",
        generalEntity="Operator",
    )
    model.entity("PipelineVersionRevision")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="OperatorRevision",
    )
    model.entity("Connection")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="Connection",
    )
    model.wasGeneratedBy(
        entity="PipelineVersionRevision",
        activity="PipelineChange",
    )
    model.entity("PipelineVersion")
    model.specializationOf(
        specificEntity="PipelineVersionRevision",
        generalEntity="PipelineVersion",
    )
    model.entity("ParentPipelineVersionRevision")
    model.wasRevisionOf(
        generatedEntity="PipelineVersionRevision",
        usedEntity="ParentPipelineVersionRevision",
    )
    model.used(
        activity="PipelineChange",
        entity="ParentPipelineVersionRevision",
    )

    return model


def create_connection_creation_model() -> prov.model.ProvDocument:
    model = prov.model.ProvDocument()
    model.set_default_namespace(uri="provinspector:")

    model.activity("PipelineChange")
    model.activity("ParentPipelineChange")
    model.wasInformedBy(
        informed="PipelineChange",
        informant="ParentPipelineChange",
    )
    model.entity("Connection")
    model.wasGeneratedBy(
        entity="Connection",
        activity="PipelineChange",
    )
    model.entity("PipelineVersionRevision")
    model.entity("OperatorRevision")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="OperatorRevision",
    )
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="Connection",
    )
    model.wasGeneratedBy(
        entity="PipelineVersionRevision",
        activity="PipelineChange",
    )
    model.entity("PipelineVersion")
    model.specializationOf(
        specificEntity="PipelineVersionRevision",
        generalEntity="PipelineVersion",
    )
    model.entity("ParentPipelineVersionRevision")
    model.wasRevisionOf(
        generatedEntity="PipelineVersionRevision",
        usedEntity="ParentPipelineVersionRevision",
    )
    model.used(
        activity="PipelineChange",
        entity="ParentPipelineVersionRevision",
    )

    return model


def create_connection_deletion_model() -> prov.model.ProvDocument:
    model = prov.model.ProvDocument()
    model.set_default_namespace(uri="provinspector:")

    model.activity("PipelineChange")
    model.activity("ParentPipelineChange")
    model.wasInformedBy(
        informed="PipelineChange",
        informant="ParentPipelineChange",
    )
    model.entity("Connection")
    model.wasInvalidatedBy(
        entity="Connection",
        activity="PipelineChange",
    )
    model.entity("PipelineVersionRevision")
    model.entity("OperatorRevision")
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="OperatorRevision",
    )
    model.hadMember(
        collection="PipelineVersionRevision",
        entity="Connection",
    )
    model.wasGeneratedBy(
        entity="PipelineVersionRevision",
        activity="PipelineChange",
    )
    model.entity("PipelineVersion")
    model.specializationOf(
        specificEntity="PipelineVersionRevision",
        generalEntity="PipelineVersion",
    )
    model.entity("ParentPipelineVersionRevision")
    model.wasRevisionOf(
        generatedEntity="PipelineVersionRevision",
        usedEntity="ParentPipelineVersionRevision",
    )
    model.used(
        activity="PipelineChange",
        entity="ParentPipelineVersionRevision",
    )

    return model


def create_operator_execution_model() -> prov.model.ProvDocument:
    model = prov.model.ProvDocument()
    model.set_default_namespace(uri="provinspector:")

    model.activity("OperatorExecution")
    model.entity("OperatorRevision")
    model.used(
        activity="OperatorExecution",
        entity="OperatorRevision",
    )
    model.entity("OperatorRun")
    model.wasGeneratedBy(
        entity="OperatorRun",
        activity="OperatorExecution",
    )
    model.entity("Metric")
    model.hadMember(
        collection="OperatorRun",
        entity="Metric",
    )
    model.hadMember(
        collection="OperatorRevision",
        entity="Metric",
    )

    return model


def create_merged_overview_model(
    models: list[prov.model.ProvDocument],
) -> prov.model.ProvDocument:
    class StrippedRelation(NamedTuple):
        s: prov.model.QualifiedName
        t: prov.model.QualifiedName
        type: Type[prov.model.ProvRelation]

    # Class mappings from PROV record type

    PROV_REC_CLS = {
        prov.model.PROV_ENTITY: prov.model.ProvEntity,
        prov.model.PROV_ACTIVITY: prov.model.ProvActivity,
        prov.model.PROV_GENERATION: prov.model.ProvGeneration,
        prov.model.PROV_USAGE: prov.model.ProvUsage,
        prov.model.PROV_COMMUNICATION: prov.model.ProvCommunication,
        prov.model.PROV_START: prov.model.ProvStart,
        prov.model.PROV_END: prov.model.ProvEnd,
        prov.model.PROV_INVALIDATION: prov.model.ProvInvalidation,
        prov.model.PROV_DERIVATION: prov.model.ProvDerivation,
        prov.model.PROV_AGENT: prov.model.ProvAgent,
        prov.model.PROV_ATTRIBUTION: prov.model.ProvAttribution,
        prov.model.PROV_ASSOCIATION: prov.model.ProvAssociation,
        prov.model.PROV_DELEGATION: prov.model.ProvDelegation,
        prov.model.PROV_INFLUENCE: prov.model.ProvInfluence,
        prov.model.PROV_SPECIALIZATION: prov.model.ProvSpecialization,
        prov.model.PROV_ALTERNATE: prov.model.ProvAlternate,
        prov.model.PROV_MENTION: prov.model.ProvMention,
        prov.model.PROV_MEMBERSHIP: prov.model.ProvMembership,
    }

    # Merge models

    mlflow2prov_overview = prov.model.ProvDocument()
    for model in models:
        mlflow2prov_overview.update(other=model)

    # Deduplicate/remove duplicate elements and relations

    mlflow2prov_overview = mlflow2prov_overview.unified()
    records = list(mlflow2prov_overview.get_records((prov.model.ProvElement)))

    bundles = dict()
    attributes = defaultdict(set)

    for relation in mlflow2prov_overview.get_records(prov.model.ProvRelation):
        stripped = StrippedRelation(
            relation.formal_attributes[0],
            relation.formal_attributes[1],
            PROV_REC_CLS[relation.get_type()],
        )
        bundles[stripped] = relation.bundle
        attributes[stripped].update(relation.extra_attributes)

    records.extend(
        relation.type(
            bundles[relation],
            None,
            [relation.s, relation.t] + list(attributes[relation]),
        )
        for relation in attributes
    )

    mlflow2prov_overview = prov.model.ProvDocument(records=records)
    mlflow2prov_overview.set_default_namespace(uri="mlflow2prov:")

    return mlflow2prov_overview


def main():
    pipeline_version_creation_model = create_pipeline_version_creation_model()
    operator_creation_model = create_operator_creation_model()
    operator_modification_model = create_operator_modification_model()
    operator_deletion_model = create_operator_deletion_model()
    connection_creation_model = create_connection_creation_model()
    connection_deletion_model = create_connection_deletion_model()
    operator_execution_model = create_operator_execution_model()
    merged_overview_model = create_merged_overview_model(
        [
            pipeline_version_creation_model,
            operator_creation_model,
            operator_modification_model,
            operator_deletion_model,
            connection_creation_model,
            connection_deletion_model,
            operator_execution_model,
        ]
    )

    for title, doc in [
        ("pipeline-version-creation", pipeline_version_creation_model),
        ("operator-creation", operator_creation_model),
        ("operator-modification", operator_modification_model),
        ("operator-deletion", operator_deletion_model),
        ("connection-creation", connection_creation_model),
        ("connection-deletion", connection_deletion_model),
        ("operator-execution", operator_execution_model),
        ("model-overview", merged_overview_model),
    ]:
        dot = prov.dot.prov_to_dot(
            bundle=doc, show_nary=False, use_labels=False, direction="BT"
        )

        dot.set_graph_defaults(
            bgcolor="transparent" if BG_TRANSPARENT else "white",
        )

        basepath = pathlib.Path(__file__).parent

        if OUTPUT_PDF:
            path = basepath / "pdf"
            pathlib.Path(path).mkdir(exist_ok=True)
            dot.write(path / f"{title}.pdf", format="pdf")
        if OUTPUT_SVG:
            path = basepath / "svg"
            pathlib.Path(path).mkdir(exist_ok=True)
            dot.write(path / f"{title}.svg", format="svg")
        if OUTPUT_DOT:
            path = basepath / "dot"
            pathlib.Path(path).mkdir(exist_ok=True)
            dot.write(path / f"{title}.dot", format="dot")


if __name__ == "__main__":
    main()
