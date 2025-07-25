import yaml
import os
from pathlib import Path

def load_cfg(cfg_file):
    cfg_file = Path(cfg_file).resolve() 
    if not cfg_file.exists():
        print(f"Config file not found: {cfg_file}")
        return {}

    with open(cfg_file, 'r') as f:
        try:
            cfg = yaml.safe_load(f)
            if cfg is None:
                print(f"Empty or invalid YAML content in {cfg_file}")
                return {}
            return cfg
        except yaml.YAMLError as exc:
            print(f"YAML parsing error in {cfg_file}: {exc}")
            return {}

def get_config_value(cfg, keys, default=None):
    val = cfg
    try:
        for key in keys:
            val = val[key]
        return val
    except (KeyError, TypeError):
        return default