import os
import requests
import json
import sys
import pandas as pd
import datetime
import wget
from loguru import logger
from workflow.scripts.utils import settings
from workflow.scripts.utils.general import copy_source_data

env_configs = settings.env_configs
data_name = "EFO"

today = datetime.date.today()

data_dir = os.path.join("/tmp", "efo")  # local test
os.makedirs(data_dir, exist_ok=True)
v = "v3.29.1"
efo_data_file = os.path.join(data_dir, f"efo-{v}.json")


def download_data():
    link = f"https://github.com/EBISPOT/efo/releases/download/{v}/efo.json"
    logger.info(link)
    wget.download(link, efo_data_file)
    copy_source_data(data_name=data_name, filename=efo_data_file)


def create_nodes():
    node_data = []
    with open(efo_data_file) as f:
        data = json.load(f)
        for g in data["graphs"]:
            logger.info(f"{len(g['nodes'])} nodes in efo.json")
            for n in g["nodes"]:
                # logger.info(json.dumps(n, indent=4, sort_keys=True))
                efo_id = n["id"]
                if "lbl" in n:
                    efo_lbl = n["lbl"].replace('\n',' ').strip()
                    efo_def = "NA"
                    if "meta" in n:
                        if "definition" in n["meta"]:
                            if "val" in n["meta"]["definition"]:
                                efo_def = n["meta"]["definition"]["val"].replace('\\n',' ').strip()
                    node_data.append(
                        {"id": efo_id, "lbl": efo_lbl, "definition": efo_def}
                    )
    logger.info(f"{len(node_data)} nodes created")
    node_df = pd.DataFrame(node_data)
    logger.info(node_df.head())
    f = os.path.join(data_dir, f"efo_nodes_{today}.csv")
    node_df.to_csv(f, index=False)
    copy_source_data(data_name=data_name, filename=f)


def create_edges():
    edge_data = []
    with open(efo_data_file) as f:
        data = json.load(f)
        for g in data["graphs"]:
            logger.info(f"{len(g['edges'])} edges in efo.json")
            for n in g["edges"]:
                # logger.info(json.dumps(n, indent=4, sort_keys=True))
                edge_data.append(n)
    logger.info(f"{len(edge_data)} edges created")
    edge_df = pd.DataFrame(edge_data)
    # logger.info(edge_df.head())
    f = os.path.join(data_dir, f"efo_edges_{today}.csv")
    edge_df.to_csv(f, index=False)
    copy_source_data(data_name=data_name, filename=f)


if __name__ == "__main__":
    download_data()
    create_nodes()
    create_edges()
