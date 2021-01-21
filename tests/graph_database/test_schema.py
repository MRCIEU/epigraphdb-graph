from pathlib import Path

import pytest
import yaml

from workflow.scripts.utils.settings import env_configs

from .models import MetaNode, MetaRel


config_path = Path(env_configs["config_path"])
schema_file = config_path / "db_schema.yaml"

with schema_file.open() as f:
    schema_dict = yaml.load(f, Loader=yaml.FullLoader)

meta_node_dict = schema_dict["meta_nodes"]
meta_rel_dict = schema_dict["meta_rels"]
meta_node_names = schema_dict["meta_nodes"].keys()
meta_rel_names = schema_dict["meta_rels"].keys()


@pytest.mark.parametrize("meta_node_name", meta_node_names)
def test_meta_node(meta_node_name):
    raw = meta_node_dict[meta_node_name]
    parsed = MetaNode(**raw)


@pytest.mark.parametrize("meta_rel_name", meta_rel_names)
def test_meta_rel(meta_rel_name):
    raw = meta_rel_dict[meta_rel_name]
    parsed = MetaRel(**raw)
