"""
Retrive EBI GWAS data from API
"""

import os
import datetime
import requests
import pandas as pd
from workflow.scripts.utils import settings
from workflow.scripts.utils.general import copy_source_data

env_configs = settings.env_configs
data_name = "ebigwas"

today = datetime.date.today()

# define file names: can't be csv (commas in vars)
ebi_gwas_data_file = f"/tmp/ebi-gwas-efo-{today}_full.tsv"
ebi_gwas_efo_mapping = f"/tmp/ebi-gwas-efo-{today}.tsv"

# file that will be used in the local mode
local_file = f"/tmp/gwas_catalog_v1.0.2-studies_r2020-11-03.tsv"


def get_ebi_gwas_data():
    # retrieve EBI GWAS data data
    ebi_gwas_api_url = (
        "https://www.ebi.ac.uk/gwas/api/search/downloads/studies_alternative"
    )
    print("Getting GWAS data from EBI GWAS Catalog", ebi_gwas_api_url)
    ebi_gwas = requests.get(ebi_gwas_api_url)
    # save the full dataset
    with open(ebi_gwas_data_file, "wb") as tsvfile:
        tsvfile.write(ebi_gwas.content)
    copy_source_data(data_name=data_name, filename=ebi_gwas_data_file)


def get_ebi_gwas_data_local():
    # use the local file when can't use API to get the data
    df = pd.read_csv(local_file, sep="\t")
    df.to_csv(ebi_gwas_data_file, sep="\t", index=False)


def select_ebi_gwas_efo_mapping():
    # keep only required columns: GWAS ID and EFO
    df = pd.read_csv(ebi_gwas_data_file, sep="\t")
    df["GWAS_ID"] = "ebi-a-" + df["STUDY ACCESSION"]
    df = df[["GWAS_ID", "MAPPED_TRAIT_URI"]].drop_duplicates()
    df.columns = ["gwas.id", "efo.id"]
    print(df.head())
    print(df.shape)
    # subset the full dataset to GWAS that are present in OpenGWAS
    df = subset_to_available_gwas(df)
    print(df.shape)
    df.to_csv(ebi_gwas_efo_mapping, sep="\t", index=False)
    copy_source_data(data_name=data_name, filename=ebi_gwas_efo_mapping)


def subset_to_available_gwas(ebi_data):
    # retrieve OpenGWAS datasets
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    print("Getting data from OpenGWAS", gwas_api_url)
    gwas_res = requests.get(gwas_api_url).json()
    dat = pd.DataFrame(gwas_res)
    # get only 'ebi-a-' ids
    gwas_all = list(dat.columns)
    gwas_ebi = [x for x in gwas_all if x.startswith("ebi-a-")]

    print("Subsetting EBI GWAS to GWAS that are present in OpenGWAS")
    ebi_keep = ebi_data[ebi_data["gwas.id"].isin(gwas_ebi)]
    return ebi_keep


if __name__ == "__main__":
    # get_ebi_gwas_data_local()
    get_ebi_gwas_data()
    select_ebi_gwas_efo_mapping()
