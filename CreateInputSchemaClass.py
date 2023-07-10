import json
# Load the JSON schema
with open(("schema.json"), 'r') as f:
    schema = json.load(f)

aggregationField = schema['aggregationField']
print(aggregationField.capitalize())
# Loop through the properties and generate the class fields and annotations
fields = []
methods = []
for prop, prop_schema in schema['properties'].items():
    field_name = prop
    field_type = 'String' if prop_schema['type'] == 'string' else 'Double'
    annotation = f'@SerializedName("{prop}")'
    field = f'{annotation}\nprivate {field_type} {field_name};\n'
    fields.append(field)

    method_name = prop
    method_type = 'String' if prop_schema['type'] == 'string' else 'Double'
    method = f'''public {method_type} get{method_name.capitalize()}() {{
        return {prop};
    }}

    public void set{method_name.capitalize()}({method_type} {prop}) {{
        this.{prop} = {prop};
    }}
    '''
    methods.append(method)

# Combine the fields into a single string
class_fields = '\n'.join(fields)

# Combine the methods into a single string
class_methods = '\n'.join(methods)


# Generate the class definition with fields and getter/setter methods
class_definition = f'''package informatik.IAMTModelling;

import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
     
    public class InputData implements Serializable{{
    
    public InputData() {{}};

    {class_fields}

    {class_methods}
}}
'''

# Print the class fields
print(class_definition)