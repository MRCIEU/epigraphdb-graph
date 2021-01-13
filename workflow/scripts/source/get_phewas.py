import os
import requests
import json
import sys
import pandas as pd
import datetime

from loguru import logger
from workflow.scripts.utils import settings
from workflow.scripts.utils.general import copy_source_data, neo4j_connect

env_configs = settings.env_configs
data_name = "phewas"

today = datetime.date.today()

variant_data = f"/tmp/variants-{today}.csv"
phewas_data_file = f"/tmp/variant-phewas-{today}.csv"

def get_variants_from_graph():
    # collect to epigraph
    driver = neo4j_connect()
    session = driver.session()
    # query
    query = """
            match (v:Variant)
            return distinct(v._id) as id limit 100
            """
    logger.info(query)
    query_data = session.run(query).data()
    df = pd.json_normalize(query_data)
    df.to_csv(variant_data, index=False)
    copy_source_data(data_name=data_name,filename=variant_data)
    return df

def get_phewas():
    df = pd.read_csv(variant_data)
    variant_ids = list(df.id)
    split_val = 20
    pval = 1e-5
    all_res = []
    for i in range(0,len(variant_ids),split_val):
        print(i)
        variants = variant_ids[i:i+split_val]
        gwas_api_url = "http://gwasapi.mrcieu.ac.uk/phewas"
        payload = {"variant": variants, "pval": pval}
        #logger.info(payload)
        response = requests.post(gwas_api_url, json=payload)
        res = response.json()
        logger.info(len(res))
        if len(res)==1:
            logger.info('Failed')
            exit()
        all_res.extend(res)
    df = pd.json_normalize(all_res)
    logger.info(df)
    df.to_csv(phewas_data_file, index=False)
    copy_source_data(data_name=data_name,filename=phewas_data_file)


if __name__ == "__main__":
    get_variants_from_graph()
    get_phewas()
