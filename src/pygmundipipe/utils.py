import yaml
from typing import Any


def load_config(config_path: str) -> dict[str, Any]:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def read_yaml(file_path: str) -> dict[str, Any]:
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)


def write_yaml(data, file_path: str) -> None:
    with open(file_path, 'w') as file:
        yaml.dump(data, file)
