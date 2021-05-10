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
data_name = "mondo"

today = datetime.date.today()

data_dir = os.path.join("/tmp", data_name)
os.makedirs(data_dir, exist_ok=True)
mondo_data_file = os.path.join(data_dir, f"Mondo-{today}.json")


def download_data():
    link = "http://purl.obolibrary.org/obo/mondo.json"
    wget.download(link, mondo_data_file)
    copy_source_data(data_name=data_name, filename=mondo_data_file)


if __name__ == "__main__":
    download_data()
