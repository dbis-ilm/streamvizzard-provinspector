from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any

from interchange.time import DateTime, Duration, Time
from prov.constants import PROV_N_MAP
from prov.identifier import Identifier, QualifiedName
from prov.model import (
    Literal,
    ProvActivity,
    ProvAgent,
    ProvBundle,
    ProvDocument,
    ProvElement,
    ProvEntity,
    ProvRelation,
)
from py2neo import Graph, Node, Relationship, Subgraph

from provinspector.storage.adapter import Adapter, Neo4JAdapter

# Constant strings
PROV_ACTIVITY = "Activity"
PROV_AGENT = "Agent"
PROV_BUNDLE = "Bundle"
PROV_ENTITY = "Entity"
PROV_TYPE = "prov:type"
PROVINSPECTOR_ID = "provinspector:identifier"
PROVINSPECTOR_LABEL = "provinspector:label"
PROVINSPECTOR_BUNDLED_IN = "provinspector:bundledIn"

# Constant tuples
PROVINSPECTOR_NODE = (PROVINSPECTOR_LABEL, "provinspector:node")
PROVINSPECTOR_EDGE = (PROVINSPECTOR_LABEL, "provinspector:edge")

# Constant mappings
EDGE_LABELS = PROV_N_MAP
NODE_LABELS = {
    ProvActivity: PROV_ACTIVITY,
    ProvAgent: PROV_AGENT,
    ProvBundle: PROV_BUNDLE,
    ProvEntity: PROV_ENTITY,
}


def str_id(
    qualified_name: QualifiedName,
):
    """
    Return PROV-N representation of a URI qualified name.
    """

    return qualified_name.provn_representation().replace("'", "")


def edge_label(
    edge: ProvRelation,
):
    """
    Return PROV-N edge label string for a given edge.
    """

    return EDGE_LABELS[edge.get_type()]


def node_label(
    node: ProvActivity | ProvAgent | ProvEntity | ProvBundle,
):
    """
    Return PROV-N node label string for a given node.
    """

    return NODE_LABELS[type(node)]


def NodePropertySet(
    node: ProvActivity | ProvAgent | ProvEntity | None = None,
    qualified_name: QualifiedName | None = None,
):
    """
    Set for node property tuples, which is used to store the properties of a PROV graph node.

    One property tuple is included in each NodePropertySet:
        - The tuple ("prov:type", "provinspector:node") is used as a primary key for merging nodes into Neo4J. This tuple always exists, even for otherwise empty nodes.

    Two more are sourced from a PROV element that can be passed as a parameter:
        - The tuple ("", node_type) is used to denote the type of PROV element that the node represents/has been sourced from.
        - The tuple ("", node_identifier) is used as a unique identifier for each node. The identifier is also used in the clients implementation of merging nodes into Neo4J.

    Before casting to a py2neo Node, the property set has to be turned into a dictionary. The conversion is
    implemented in `to_property_dict`.
    """

    if node is None and qualified_name is None:
        # Should be avoided as every node needs to have a property value for PROVINSPECTOR_ID to be succesfully merged into Neo4J
        return {
            PROVINSPECTOR_NODE,
        }

    if node is None and qualified_name is not None:
        return {
            PROVINSPECTOR_NODE,
            (PROVINSPECTOR_ID, str_id(qualified_name)),
        }

    properties = {} if type(node) is ProvBundle else node.attributes  # type:ignore
    ident = (PROVINSPECTOR_ID, str_id(node.identifier))  # type:ignore
    label = (PROVINSPECTOR_LABEL, node_label(node))  # type:ignore

    return {
        PROVINSPECTOR_NODE,
        label,
        ident,
        *properties,
    }


def EdgePropertySet(
    edge: ProvRelation | None = None,
):
    """
    Set for edge property tuples, which is used to store the properties of a PROV graph edge.

    One property tuple is included in each EdgePropertySet:
        - The tuple ("prov:type", "provinspector:edge") denoting that the relationship encodes an edge in the PROV graph.

    One more tuple is included when data can be sourced from a PROV relation:
        - The tuple ("prov:type", edge_type) is used to encode the type of edge/relationship that the edge represents.

    Before being cast to a py2neo Relationship the property set needs to be turned into a dictionary. The conversion is the same as for the NodePropertySet and is implemented in `to_property_dict`.
    """

    if edge is None:
        return {
            PROVINSPECTOR_EDGE,
        }

    label = (PROVINSPECTOR_LABEL, edge_label(edge))
    properties = [*edge.attributes[2:], *edge.extra_attributes]

    return {
        PROVINSPECTOR_EDGE,
        label,
        *properties,
    }


def encode_value(
    value: Any,
):
    """
    Encode a property value as a Neo4J (py2neo) primitive.
    """

    if type(value) in (QualifiedName, Identifier):
        return str(value)
    elif type(value) is Literal:
        return value.provn_representation()
    elif type(value) in (date, datetime):
        return DateTime.from_native(value)
    elif type(value) is time:
        return Time.from_native(value)
    elif type(value) is timedelta:
        return Duration(seconds=int(value.total_seconds()))

    return value


def encode_graph(
    graph: ProvDocument,
):
    """
    Encode a PROV graph (ProvDocument) as a collection of py2neo Nodes and Relationships (Subgraph).
    """

    nodes = encode_nodes(graph)
    edges = encode_edges(graph, nodes)
    edges.extend(encode_bundle_edges(graph, nodes))

    return Subgraph(nodes.values(), edges)


def get_graph_nodes(
    graph: ProvDocument,
):
    """
    Return a list containing the nodes of a PROV graph.

    Explore the graph using BFS level by level. Expand bundles and add bundle nodes to the queue. A deque is used as queue.

    Bundles are included in the returned nodes. Notice that while not all bundles are PROV elements the ones that are are PROV entities. Most of the time, the declaration for the entity part and the bundle part are stored in two different PROV elements.
    """

    nodes = []
    q = deque([graph.get_records(ProvElement), graph.bundles])

    while q:
        current_bfs_level = q.popleft()

        for item in current_bfs_level:
            nodes.append(item)

            if type(item) is ProvBundle:
                q.append(item.get_records(ProvElement))
                q.append(list(item.bundles))

    return nodes


def get_graph_edges(
    graph: ProvDocument,
):
    """
    Return a list containing the edges of a PROV graph.

    Flatten the graph and return its records filtered for edges (ProvRelations).
    """

    flattened = graph.flattened()

    return list(flattened.get_records(class_or_type_or_tuple=ProvRelation))


def encode_nodes(
    graph: ProvDocument,
):
    """
    Encode the nodes of a PROV graph as py2neo Nodes. Return a mapping from node id to py2neo Node.

    Create a NodePropertySet for each PROV type contained in the list of nodes provided by `get_graph_nodes`.

    RDF allows literal values to be the endpoint of relations. As these literals do not represent nodes themselves, we turn those relationships into node properties as follows:

        - From: (Node) -{relation}-> Literal
        - To: (Node {relation: Literal})

    Finally, the computed NodePropertySets are cast to py2neo nodes using the `to_py2neo_node` function before returning the mapping from node ids to py2neo nodes.
    """

    nodes = {}

    for node in get_graph_nodes(graph):
        nodes[str_id(node.identifier)] = NodePropertySet(node)

    for edge in get_graph_edges(graph):
        (_, source) = edge.formal_attributes[0]
        (_, target) = edge.formal_attributes[1]

        # Create source node if it does not exist
        source_id = str_id(source)

        if source_id not in nodes:
            nodes[source_id] = NodePropertySet(qualified_name=source)

        # Handle edges of type: (Node) -relation-> Literal
        if type(target) is not QualifiedName:
            key = edge_label(edge)
            val = encode_value(target)
            nodes[source_id].update({(key, val)})
            continue

        # Create target node if it does not exist
        target_id = str_id(target)
        if target_id not in nodes:
            nodes[target_id] = NodePropertySet(qualified_name=target)

    # Convert property sets to py2neo nodes
    for node_id, node_property_set in list(nodes.items()):
        nodes[node_id] = to_py2neo_node(node_property_set)

    return nodes


def to_property_dict(
    property_set: set,
):
    """
    Turn a property set into a dict of properties.
    """

    # Encode the set of properties
    encoded_property_set = [tuple(map(encode_value, item)) for item in property_set]

    # Count the occurence of each property key
    key_count = Counter(key for key, _ in encoded_property_set)

    property_dict = defaultdict(list)

    for key, value in encoded_property_set:
        # Property is single-valued iff the property key occurs only once
        if key_count.get(key) == 1:
            property_dict[key] = value  # type:ignore

        # Property is multi-valued iff the property key occurs more than once

        # Values of a multi-valued property are stored in a list
        else:
            property_dict[key].append(value)

    return property_dict


def to_py2neo_node(
    property_set: set,
):
    """
    Turn a property set into a py2neo Node.

    First convert the property set into a property dict. Then extract the node labels from the dict. Node labels are the values stored at the key `prov:type`.
    """

    property_dict = to_property_dict(property_set)

    # Get node labels, key PROVINSPECTOR_LABEL
    labels = property_dict[PROVINSPECTOR_LABEL]

    # Delete the key PROVINSPECTOR_LABEL
    del property_dict[PROVINSPECTOR_LABEL]

    if type(labels) is not list:
        return Node(labels, **property_dict)

    return Node(*labels, **property_dict)


def encode_edges(
    graph: ProvDocument,
    nodes: dict,
):
    """
    Encode the edges of a PROV graph as py2neo Relationships (edges). Returns a list of py2neo Relationships.

    Encode the attributes of each edge as an EdgePropertySet. Turn the property set into a property dict. Create the py2neo relationship and add it to the list of edges.

    Ignore/skip edges that connect a node to a literal, as these have been handled in the node encoding already.

    Use the provided mapping from node ids to py2neo Nodes as a lookup for source and target nodes of each created edge.
    """

    edges = []

    for edge in get_graph_edges(graph):
        (_, source) = edge.formal_attributes[0]
        (_, target) = edge.formal_attributes[1]

        if type(target) is QualifiedName:
            source = nodes[str_id(source)]
            target = nodes[str_id(target)]

            label = edge_label(edge)
            properties = EdgePropertySet(edge)
            encoded_properties = to_property_dict(properties)

            relation = Relationship(source, label, target, **encoded_properties)
            edges.append(relation)

    return edges


def encode_bundle_edges(
    graph: ProvDocument,
    nodes: dict,
):
    """
    Return a list of `bundledIn` edges between each bundle node and the nodes included within the bundle.

    If a node (A) is included in a bundle that is encoded in node (B), a relationship/edge of type `bundledIn` is created between the two. This allows to retrieve the nodes that are included in a specific bundle with a simple cypher query.
    """

    edges = []

    for node in get_graph_nodes(graph):
        # Skip bundles as ProvBundle.bundle would raise an AttributeError
        # Skip documents for the same reason
        if type(node) in (ProvBundle, ProvDocument):
            continue

        if type(node.bundle) is ProvBundle:
            n_id = str_id(node.identifier)
            b_id = str_id(node.bundle.identifier)  # type:ignore

            source = nodes[n_id]
            target = nodes[b_id]

            relation = Relationship(source, PROVINSPECTOR_BUNDLED_IN, target)
            edges.append(relation)

    return edges


def add_id_uniqueness_constraints(
    graph: Graph,
) -> None:
    """
    Add uniqueness constraints to the property key `id` for all basic PROV types (e.g., ProvActivity, ProvAgent, ProvEntity, and ProvBundle).
    """

    if graph is None:
        return

    # property_key="id"
    property_key = PROVINSPECTOR_ID

    for label in NODE_LABELS.values():
        if property_key not in graph.schema.get_uniqueness_constraints(  # type:ignore
            label
        ):
            graph.schema.create_uniqueness_constraint(  # type:ignore
                label,
                property_key,
            )

    # for label in NODE_LABELS.values():
    #     property_key = "id"
    #
    #     label = cypher_escape(label)
    #     label = cypher_escape(label)
    #
    #     cypher = f"CREATE CONSTRAINT ON (_:{label}) ASSERT _.{property_key} IS UNIQUE"
    #
    #     graph.update(cypher)


@dataclass
class ProvGraphDatabase:
    adapter: Adapter = field(default_factory=Neo4JAdapter)

    def __post_init__(self):
        # Add uniqueness constraints
        add_id_uniqueness_constraints(self.adapter.graph)

    def shutdown(self) -> None:
        self.adapter.shutdown()

    def import_graph(self, graph: ProvDocument) -> None:
        """
        Import a PROV graph into a graph database.
        """

        if self.adapter.graph is None:
            return

        # Encode graph as py2neo Subgraph
        encoded_graph = encode_graph(graph)

        # Node identifier acts as primary key for merge
        primary_key = PROVINSPECTOR_ID
        # `provinspector:node` acts as primary label for merge
        primary_label = PROVINSPECTOR_NODE[1]

        tx = self.adapter.graph.begin()

        # Merge all nodes and edge, merge updates already existing nodes, and create new ones if necessary
        tx.merge(encoded_graph, primary_label=primary_label, primary_key=primary_key)

        self.adapter.graph.commit(tx)

    def clear(self) -> None:
        """
        Clear database.
        """

        if self.adapter.graph is None:
            return

        self.adapter.graph.run(
            cypher="""MATCH (n) DETACH DELETE n""",
        )
