# inbuilt
import configparser
from dataclasses import dataclass


@dataclass
class LoadConfig:
    config: configparser.ConfigParser
    feed_specs: dict
    master_specs: dict
    target_unity_catalog: str
    target_schema_name: str
    target_table_name: str
