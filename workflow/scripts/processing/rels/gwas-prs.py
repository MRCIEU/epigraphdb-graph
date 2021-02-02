import os
import re
import gzip
import requests
import pandas as pd
import sys
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

FILE1 = get_source(meta_id,1)
FILE2 = get_source(meta_id,2)

def get_prs_gwas_ids():
    idList = []
    with open(os.path.join(dataDir, FILE2)) as f:
        for line in f:
            l = line.rstrip()
            if l.startswith("UKB"):
                idList.append(l.lower().replace(":", "-"))
            else:
                idList.append("ieu-a-" + l)
    return idList


def get_gwas_data():
    print("Getting gwas data...")
    gFile = os.path.join(dataDir, "gwas-data.tsv")
    if os.path.exists(gFile):
        print("Already done")
        df = pd.read_csv(gFile, sep="\t")
    else:
        # create the data
        gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
        gwas_res = requests.get(gwas_api_url).json()
        outData = open(gFile, "w")
        df = pd.DataFrame(gwas_res)
        df = df.T.fillna("")
        print(df.head())
        print(df["year"].describe())
        df.to_csv(outData, sep="\t", index=False)
        outData.close()
    return df


def process():
    df = get_gwas_data()
    idList = get_prs_gwas_ids()
    logger.info("\n{}", df.head())
    prs_df = pd.read_csv(os.path.join(dataDir, FILE1), sep="|")
    logger.info("\n{}", prs_df.head())
    PRSDic = {
        "Alpha-linolenic acid (18:3n3)": "ieu-a-1145",
        "Alzheimers disease": "ieu-a-297",
        "AUCins_AUCglu": "ieu-a-759",
        "Crohns disease": "ieu-a-30",
        "Fathers age at death": "ieu-a-1092",
        "Mothers age at death": "ieu-a-1093",
        "Pagets disease": "ieu-a-975",
        "Parents age at death": "ieu-a-1094",
        "Parkinsons disease": "ieu-a-818",
        "Type 2 diabetes": "ieu-a-23",
        "Urate": "ieu-a-1055",
    }
    outComeDic = {
        "Diagnoses - main ICD10: K30 Dyspepsia": ["ukb-b-14814"],
        "Prospective memory result 2nd attempt": ["ukb-b-4282", "ukb-a-197"],
        "Alcohol intake frequency": ["ukb-b-5779", "ukb-a-25"],
        "Prospective memory result 1st attempt": ["ukb-b-4282", "ukb-a-197"],
    }
    for i in prs_df["PRS"].unique():
        c = df[(df["trait"].replace("'", "") == i) & (df["id"].isin(idList))]
        # c = df[(df['trait']==i)]
        num = c.shape[0]
        if num < 1 or num > 1:
            logger.info("PRS i {}", i)
            logger.info(c.shape[0])
            logger.info(c["id"])
        else:
            dd = c["id"].values.tolist()
            for d in dd:
                # print(len(d))
                PRSDic[i] = d
    for i in prs_df["Outcome"].unique():
        # print(df['trait'])
        # c = df[(df['trait']==i)]
        c = df[(df["trait"] == i) & (df["id"].str.startswith("ukb-"))]
        num = c.shape[0]
        if num < 1:
            logger.info("Outcome i {}", i)
            logger.info(c.shape[0])
            logger.info(c["id"])
        else:
            dd = c["id"].values.tolist()
            for d in dd:
                # print(len(d))
                if i in outComeDic:
                    outComeDic[i].append(d)
                else:
                    outComeDic[i] = [d]
    # print(PRSDic)
    # print(outComeDic)
    prs_df["PRS_GWAS_ID"] = prs_df["PRS"].map(PRSDic)
    prs_df["Outcome_GWAS_ID"] = prs_df["Outcome"].map(outComeDic)
    # drop rows with nan as dataframe is littered with headers
    prs_df = prs_df.dropna()
    csv_data = []
    for i, row in prs_df.iterrows():
        # print(row)
        for i in row["Outcome_GWAS_ID"]:
            d = list(row.values[0:-1])
            # print(d,i)
            d.append(i)
            csv_data.append(d)

    colnames = [
        "gwasname1",
        "nsnps",
        "model",
        "gwasname2",
        "beta",
        "se",
        "p",
        "r2",
        "n",
        "source",
        "target",
    ]
    df = pd.DataFrame(csv_data, columns=colnames)
    logger.info("\n{}", df.head())
    df.drop(columns=["gwasname1", "gwasname2"], inplace=True)
    logger.info("\n{}", df.head())
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    logger.info("\n{}", df.head())
    # match columns to expexted types
    df = df.astype(
        {
            "nsnps": "int64",
            "beta": "float",
            "se": "float",
            "p": "float",
            "r2": "float",
            "n": "int64",
        }
    )
    logger.info(df.dtypes)
    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
