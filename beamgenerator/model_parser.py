# 1. create source node code
def add_java_source_node(source_node_id, source_node): 
    return f'''
        PCollection<InputData{source_node_id.capitalize()}> {source_node_id} = pipeline
            .apply("Read from Kafka", kafkaRead(options, "{source_node['kafkaTopic']}", consumerConfig))
            .apply(Values.create())
            .apply(window({source_node['windowlengthInSec']}, {source_node['slidingWindow']}, {source_node['slidingWindowStepInSec']}))
            .apply("Parse JSON to InputData{source_node_id.capitalize()}", ParDo.of(new DoFn<String, InputData{source_node_id.capitalize()}>() {{
                @ProcessElement
                public void processElement(ProcessContext c) {{
                    String jsonLine = c.element(); 
                    InputData{source_node_id.capitalize()} inputdata = new Gson().fromJson(jsonLine, InputData{source_node_id.capitalize()}.class);
                    c.output(inputdata);
                }}
            }}));

    '''

def add_java_join_node(join_node_id, join_node: dict, node_map: dict):
    # TODO: if join of PCollection of type InputDataXYZ => need to cast all to one
    join_nodes = [node_map[node_id] for node_id in join_node["join_list"]]
    # determine input type for join node
    # check if all join nodes have equal dict entry "output_p_collection_are the same


    # TODO: implement join logic
    return f'''
        PCollection<InputData> {join_node_id};
    '''

def add_java_transform_node(transform_node_id, transform_node: dict, node_map: dict):
    # TODO: implement transform logic
    return f'''
        PCollection<InputData> {transform_node_id};
    '''

def add_java_sink_node(sink_node_id, sink_node: dict, node_map: dict):
    # TODO. implement sink node logic
    return f'''
        PCollection<InputData> {sink_node_id};
    '''


def get_next_node(node: dict, pending_visit: set, node_map: dict):
    # set input type for next nodes
    for next_node in [node_map[node_id] for node_id in node["next_oiid"]]:
        next_node["input_p_collection_type"].append(node["output_p_collection_type"])

    # return one next node and add rest to pending_visit set
    if len(node["next_oiid"]) > 1:
        pending_visit.update(node["next_oiid"][1:])
    elif len(node["next_oiid"]) < 1:
        raise Exception("Node has no next node")
    return node["next_oiid"][0], node_map[node["next_oiid"][0]]