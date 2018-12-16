import json


def load_config(config_path='config.json'):
    try:
        with open(config_path) as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(
            f'Config file "{config_path}" not found!'
            'Please create a config file or check whether it exists.'
        )
