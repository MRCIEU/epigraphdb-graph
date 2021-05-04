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
data_name = "CPIC"

today = datetime.date.today()

data_dir = os.path.join("/tmp", "cpic")  # local test
os.makedirs(data_dir, exist_ok=True)
cpic_data_file = os.path.join(data_dir, f"cpicPairs-{today}.csv")


def download_data():
    link = 'https://api.cpicpgx.org/data/cpicPairs.csv'
    wget.download(link, cpic_data_file)
    copy_source_data(data_name=data_name, filename=cpic_data_file)

if __name__ == "__main__":
    download_data()
