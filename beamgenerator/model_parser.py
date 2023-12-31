# 1. create source node code
from beamgenerator.node import Node
from beamgenerator.types import AbstractPCollectionType, InputPCollectionType, SimplePCollectionType, JAVA_NUMERIC_TYPE

def add_source_java_code(source_node: Node) -> str: 
    return f'''
        PCollection<{source_node.output_p_collection_type.type}> {source_node.output_p_collection_type.name} = pipeline
            .apply("Read from Kafka", kafkaRead(options, "{source_node.kafka_topic}", consumerConfig))
            .apply(Values.create())
            .apply(window({source_node.window_length_in_sec}, {source_node.is_sliding_window()}, {source_node.get_sliding_window_value()}))
            .apply("Parse JSON to {source_node.output_p_collection_type.type}", ParDo.of(new DoFn<String, {source_node.output_p_collection_type.type}>() {{
                @ProcessElement
                public void processElement(ProcessContext c) {{
                    String jsonLine = c.element(); 
                    {source_node.output_p_collection_type.type} inputData = new Gson().fromJson(jsonLine, {source_node.output_p_collection_type.type}.class);
                    c.output(inputData);
                }}
            }}));

    '''

def add_join_java_code(join_node: Node, node_map: dict[str, Node]) -> str:
    # check if all nodes to be joined have equal output_p_collection_are
    input_types : list[AbstractPCollectionType] = join_node.input_p_collection_types
    print("Add join node" + str(join_node.node_id))
    output_type = input_types[0]
    for input_type in input_types[1:]:
        if input_type != output_type:
            raise Exception(f'Join nodes {join_node.join_list} have different output types: {input_type} does not match {output_type}.')
        
    # set input type for next nodes
    output_pcol_name = join_node.node_id
    for next_node in [node_map[nodeid] for nodeid in join_node.next_oiid]:
        next_node.add_input_p_collection_type(output_type, output_pcol_name)

    appendix_code = ''
    changed = False
    if join_node.is_transform():
        appendix_code += add_transform_java_code(join_node, node_map)
        changed = True
    elif join_node.is_output():
        appendix_code += add_sink_java_code(join_node, node_map)
        changed = True
    if changed: 
        # join_node.node_id is name of output PCollection of the transform or sink node
        output_pcol_name += "Join"
    join_code = f'''
        PCollection<{output_type.type}> {output_pcol_name} = PCollectionList.of({").and(".join([node_id for node_id in join_node.join_list])})
            .apply(Flatten.<{output_type.type}>pCollections());
        ''' 
    return join_code + appendix_code

def add_transform_java_code(transform_node: Node, node_map: dict[str, Node]) -> str:
    # TODO: check input data type
    output_pcol_name = transform_node.node_id
    output_pcol_type = "Double"
    input_pcol_name = transform_node.input_p_collection_types[0].name
    input_type = transform_node.input_p_collection_types[0]
    code = f'''
        PCollection<{output_pcol_type}> {output_pcol_name} = {input_pcol_name}'''
    if transform_node.name == 'Mean':
        input_type = transform_node.input_p_collection_types[0]
        if isinstance(input_type, SimplePCollectionType):
            if input_type.type not in JAVA_NUMERIC_TYPE:
                raise Exception(f'Mean transform only works on numeric types {JAVA_NUMERIC_TYPE}. Type {input_type.type} is not supported.')
            else:
                code += f'''
            .apply("Average in Window", Mean.<Double>globally().withoutDefaults());
        '''

        elif isinstance(input_type, InputPCollectionType):
            field_to_avg = next(param["value"] for param in transform_node.parameter_list if param["name"] == "FieldToAverage")
            type_of_field_to_avg = input_type.fields[field_to_avg]
            if type_of_field_to_avg not in JAVA_NUMERIC_TYPE:
                raise Exception(f'Mean transform only works on numeric types {JAVA_NUMERIC_TYPE}. Type {type_of_field_to_avg} is not supported.')
            else:
                code += f'''
            .apply("Parse {field_to_avg}", MapElements.into(TypeDescriptors.doubles())
                .via(({input_type.type} inputData) -> inputData.get{field_to_avg.capitalize()}().doubleValue()))
            .apply("Average {field_to_avg} in Window", Mean.<Double>globally().withoutDefaults());
        '''
        transform_node.output_p_collection_type = SimplePCollectionType(output_pcol_type, output_pcol_name)
        return code
    else:
        raise Exception(f'Transform {transform_node.name} not supported.')

def add_sink_java_code(sink_node: Node, node_map: dict[str, Node]) -> str:
    
    print("Add sink code for " + str(sink_node.node_id))
    # TODO: implement sink node logic
    input_pcol_name = sink_node.input_p_collection_types[0].name
    outputPColl = "outputPColl" + sink_node.node_id.capitalize()
    # format output collection
    code = f'''
        PCollection<String> {outputPColl} = {input_pcol_name}.apply("Convert to JSON String", MapElements.into(TypeDescriptors.strings())
            .via((inputData) -> new Gson().toJson(inputData)));'''
    
    if sink_node.name == 'KafkaOutput':
        return code + f'''
        {outputPColl}.apply("Write to Kafka", kafkaWrite(options, "{sink_node.kafka_topic}"));
        '''
    
    elif sink_node.name == 'DeltaOutput':
        raise Exception(f'DeltaOutput not supported yet.')
    
    elif sink_node.name == 'ConsoleOutput':
        return code + f'''
        {outputPColl}.apply("Write to Console", ParDo.of(new DoFn<String, Void>() {{
            @ProcessElement
            public void processElement(ProcessContext c) {{
                System.out.println(c.element());
            }}
        }}));
        '''
    
    elif sink_node.name == 'FileOutput':
        return code + f'''
        {outputPColl}.apply("Write to File", TextIO.write().to("{sink_node.output_file}").withWindowedWrites());
        '''
    else:
        raise Exception(f'Sink node {sink_node.name} not supported.')


def get_next_node(current_node: Node, pending_visit: set, node_map: dict[str, Node], join_nodes: dict[str, Node]) -> Node:
    if not current_node.is_output():
        # set input type for next nodes
        for next_node in [node_map[nodeid] for nodeid in current_node.next_oiid]:
            next_node.add_input_p_collection_type(current_node.output_p_collection_type, current_node.node_id)

        # return one next node and add rest to pending_visit set
        if len(current_node.next_oiid) > 1:
            pending_visit.update(current_node.next_oiid[1:])
            # Note: if a child of current node is in join_nodes we remove it and process it sooner either as next node or in pending_visit list
            for node_id in current_node.next_oiid:
                join_nodes.pop(node_id, None)
        return node_map[current_node.next_oiid[0]]
    else:
        raise Exception(f'Node {current_node.node_id} has no next node!')