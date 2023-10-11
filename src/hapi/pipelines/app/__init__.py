from os.path import join

from hdx.utilities.loader import load_and_merge_yaml
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.saver import save_yaml


def compile_YAMLs(config_files):
    input_files = [
        script_dir_plus_file(join("..", "configs", file), compile_YAMLs)
        for file in config_files
    ]
    project_config = load_and_merge_yaml(input_files)
    output_file = script_dir_plus_file(
        "project_configuration.yaml", compile_YAMLs
    )
    save_yaml(project_config, output_file)
    return output_file
