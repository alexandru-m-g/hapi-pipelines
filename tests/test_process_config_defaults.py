from hdx.utilities.loader import load_and_merge_yaml

from hapi.pipelines.utilities.process_config_defaults import add_defaults


def test_process_config_defaults():
    config_dict = load_and_merge_yaml(
        ["tests/test_process_config_defaults.yaml"]
    )
    config_dict = add_defaults(config_dict)
    expecdted_config_dict = {
        "theme_name_national": {
            "theme_name_foo": {
                "other_parmas": "pass",
                "input": ["c4", "c1", "c2", "c3"],
                "list": ["c4", "c1", "c2", "c3"],
                "output": ["c4", "c1", "c2", "c3"],
                "output_hxl": ["#c4", "#c1", "#c2", "#c3"],
            },
            "theme_name_bar": {
                "other_parmas": "pass",
                "input": ["c1", "c2", "c3"],
                "list": ["c1", "c2", "c3"],
                "output": ["c1", "c2", "c3"],
                "output_hxl": ["#c1", "#c2", "#c3"],
            },
            "theme_name_no_default": {
                "input": ["c5"],
                "list": ["c5"],
                "output": ["c5"],
                "output_hxl": ["#c5"],
            },
        }
    }

    assert config_dict == expecdted_config_dict
