import argparse
import os
import shutil
from jinja2 import Environment, FileSystemLoader

# Get the absolute path of the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the template file

path=os.path.join(current_dir, 'beam-template', 'src', 'main', 'java', '{{PACKAGE_NAME}}')

# Create the Jinja2 environment
templateLoader = FileSystemLoader(searchpath=path)
templateEnv = Environment(loader=templateLoader)
TEMPLATE_FILE = "{{PROJECT_NAME}}Pipeline.java"

# Load the template file
template = templateEnv.get_template(TEMPLATE_FILE)

# Create the argument parser
parser = argparse.ArgumentParser(description='Generate an Apache Beam project based on a template')
parser.add_argument('project_name', help='Name of the project')
parser.add_argument('package_name', help='Package name for the project')
parser.add_argument('--target_dir', default='my-project', help='Target directory for the generated project')

# Parse the command-line arguments
args = parser.parse_args()

# Assign the parsed values to variables
project_name = args.project_name
package_name = args.package_name
target_dir = args.target_dir


# Load and process the template file
output = template.render(PACKAGE_NAME=package_name, PROJECT_NAME=project_name)

# Create the target directory
target_path = os.path.join(target_dir, 'src/main/java', package_name.replace('.', '/'))
os.makedirs(target_path, exist_ok=True)


# Write the processed template to the target file
target_file = os.path.join(target_path, f'{project_name}Pipeline.java')

with open(target_file, 'w') as f:
    f.write(output)

# Copy the pom.xml file to the output directory
pom_file_src = os.path.join(current_dir, 'beam-template', 'pom.xml')
pom_file_dst = os.path.join(target_dir, 'pom.xml')
shutil.copy2(pom_file_src, pom_file_dst)
