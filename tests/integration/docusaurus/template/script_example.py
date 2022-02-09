import os

from ruamel import yaml

yaml = yaml.YAML(typ="safe")

import great_expectations as ge

context = ge.get_context()

assert context
