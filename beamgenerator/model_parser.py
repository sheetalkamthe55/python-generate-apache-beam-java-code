# 1. create source node code
from beamgenerator.node import Node


def add_source_java_code(source_node: Node): 
    return f'''
        PCollection<{source_node.output_p_collection_type.name}> {source_node.node_id} = pipeline
            .apply("Read from Kafka", kafkaRead(options, "{source_node.kafka_topic}", consumerConfig))
            .apply(Values.create())
            .apply(window({source_node.window_length_in_sec}, {source_node.is_sliding_window}, {source_node.sliding_window_step_in_sec}))
            .apply("Parse JSON to {source_node.output_p_collection_type.name}", ParDo.of(new DoFn<String, {source_node.output_p_collection_type.name}>() {{
                @ProcessElement
                public void processElement(ProcessContext c) {{
                    String jsonLine = c.element(); 
                    {source_node.output_p_collection_type.name} inputdata = new Gson().fromJson(jsonLine, {source_node.output_p_collection_type.name}.class);
                    c.output(inputdata);
                }}
            }}));

    '''

def add_join_java_code(join_node: Node, node_map: dict[str, Node]):
    # TODO: if join of PCollection of type InputDataXYZ => need to cast all to one
    join_nodes = [node_map[nodeid] for nodeid in join_node.join_list]
    # determine input type for join node
    # check if all join nodes have equal dict entry "output_p_collection_are the same


    # TODO: implement join logic
    return f'''
        PCollection<InputData> {join_node.node_id};
    '''

def add_transform_java_code(transform_node: Node, node_map: dict[str, Node]):
    # TODO: implement transform logic
    return f'''
        PCollection<InputData> {transform_node.node_id};
    '''

def add_sink_java_code(sink_node: Node, node_map: dict[str, Node]):
    # TODO. implement sink node logic
    return f'''
        PCollection<InputData> {sink_node.node_id};
    '''


def get_next_node(node: Node, pending_visit: set, node_map: dict[str, Node]) -> Node:
    if not node.is_output():
        # set input type for next nodes
        for next_node in [node_map[nodeid] for nodeid in node.next_oiid]:
            next_node.add_input_p_collection_type(node.output_p_collection_type)

        # return one next node and add rest to pending_visit set
        if len(node.next_oiid) > 1:
            pending_visit.update(node.next_oiid[1:])
        return node_map[node.next_oiid[0]]
    else:
        raise Exception(f'Node {node.node_id} has no next node!')