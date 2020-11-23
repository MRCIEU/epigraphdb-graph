import os
import re
import gzip
import json
import sys
import csv
import pandas as pd
import requests

from loguru import logger

#################### leave me heare please :) ########################

from workflow.scripts.utils.general import setup, get_source

from workflow.scripts.utils.writers import (
    create_constraints,
    create_import,
)

# setup
args, dataDir = setup()
meta_id = args.name

# args = the argparse arguments (name and data)
# dataDir = the path to the working directory for this node/rel

#######################################################################

FILE = get_source(meta_id,1)


def get_gwas_data():
    print("Getting GWAS IDs...")
    # create the data
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    gwas_res = requests.get(gwas_api_url).json()
    idDic = {}
    ukb_a = {}
    ukb_b = {}
    for g in gwas_res:
        # get some info on the ukb-a traits as need to match trait text
        if gwas_res[g]["id"].startswith("ukb-a") or gwas_res[g]["id"].startswith(
            "ukb-d"
        ):
            ukb_a[gwas_res[g]["trait"]] = gwas_res[g]["id"]
        elif gwas_res[g]["id"].startswith("ukb-b"):
            note = gwas_res[g]["note"].split(":")[0].replace("#", "_")
            idDic[note] = [gwas_res[g]["id"]]
            ukb_b[gwas_res[g]["id"]] = gwas_res[g]["trait"]

    # match our ukb traits to BN on trait name - not ideal but not sure what else to do...?
    for note in idDic:
        for t in idDic[note]:
            if t in ukb_b:
                trait_text = ukb_b[t]
                # print(t,trait_text)
                if trait_text in ukb_a:
                    # print(ukb_a[trait_text])
                    idDic[note].append(ukb_a[trait_text])
    print(len(idDic))
    # for i in idDic:
    #    if len(idDic[i])>1:
    #        print(i,idDic[i])
    return idDic


def process(idDic):
    logger.info("Reading {}", FILE)
    df = pd.read_csv(os.path.join(dataDir, FILE), delimiter="\s+")
    logger.info(df.head())
    logger.info(df.shape)

    # ignore rows with NA for rg
    df = df[df["rg"] != "NA"]
    logger.info(df.shape)

    # filter by _irnt and not _raw
    df = df[
        (df["p1"].str.contains("_irnt")) & (df["p2"].str.contains("_irnt"))
        | (~df["p1"].str.contains("_raw")) & (~df["p2"].str.contains("_raw"))
    ]
    logger.info(df.head())
    logger.info(df.shape)

    # modify p1 and p2 names
    p1_trim = (
        df["p1"]
        .str.split("/", expand=True)
        .iloc[:, -1]
        .str.replace("_irnt", "")
        .str.split(".", expand=True)[0]
    )
    p2_trim = (
        df["p2"]
        .str.split("/", expand=True)
        .iloc[:, -1]
        .str.replace("_irnt", "")
        .str.split(".", expand=True)[0]
    )
    df["p1_trim"] = p1_trim
    df["p2_trim"] = p2_trim
    logger.info(df.head())
    logger.info(df.shape)

    # match to OpenGwas names

    # create dataframe using opengwas ID dictionary then merge with df
    # https://stackoverflow.com/questions/53480403/how-to-merge-pandas-dataframe-with-dict-of-lists
    gwas_df = pd.DataFrame.from_dict(idDic, orient="index").unstack().reset_index()
    logger.info(gwas_df.head())
    df = pd.merge(
        df, gwas_df[gwas_df[0].notnull()], left_on=["p1_trim"], right_on=["level_1"]
    )
    df = df.rename(columns={0: "p1_gwas"})
    df.drop(["level_0", "level_1"], axis=1, inplace=True)
    df = pd.merge(
        df, gwas_df[gwas_df[0].notnull()], left_on=["p2_trim"], right_on=["level_1"]
    )
    df = df.rename(columns={0: "p2_gwas"})
    df.drop(
        ["level_0", "level_1", "p1", "p2", "p1_trim", "p2_trim"], axis=1, inplace=True
    )
    logger.info("\n{}", df.head())
    logger.info(df.shape)

    # drop dupls
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)

    # set source and target
    df = df.rename(columns={"p1_gwas": "source", "p2_gwas": "target"})
    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    idDic = get_gwas_data()
    process(idDic)