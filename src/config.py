from pathlib import Path

import yaml


def load_config():
    config_path = Path("config/project.yaml")

    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)