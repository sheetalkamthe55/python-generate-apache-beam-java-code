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
    code = f'''
        PCollection<InputData> {join_node.node_id} some join logic;
    '''
    # TODO: check logic
    if join_node.is_transform():
        code += add_transform_java_code(join_node, node_map)
    elif join_node.is_output():
        code += add_sink_java_code(join_node, node_map)
    return code

def add_transform_java_code(transform_node: Node, node_map: dict[str, Node]):
    # TODO: implement transform logic
    return f'''
        PCollection<InputData> {transform_node.node_id} some transform logic;
    '''

def add_sink_java_code(sink_node: Node, node_map: dict[str, Node]):
    # TODO. implement sink node logic
    return f'''
        PCollection<InputData> {sink_node.node_id} some sink logic;
    '''


def get_next_node(current_node: Node, pending_visit: set, node_map: dict[str, Node], join_nodes: dict[str, Node]) -> Node:
    if not current_node.is_output():
        # set input type for next nodes
        for next_node in [node_map[nodeid] for nodeid in current_node.next_oiid]:
            next_node.add_input_p_collection_type(current_node.output_p_collection_type)

        # return one next node and add rest to pending_visit set
        if len(current_node.next_oiid) > 1:
            pending_visit.update(current_node.next_oiid[1:])
            # Note: if a child of current node is in join_nodes we remove it and process it sooner either as next node or in pending_visit list
            for node_id in current_node.next_oiid:
                join_nodes.pop(node_id, None)
        return node_map[current_node.next_oiid[0]]
    else:
        raise Exception(f'Node {current_node.node_id} has no next node!')