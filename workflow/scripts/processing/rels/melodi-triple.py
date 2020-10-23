import os
import sys
import pandas as pd
import numpy as np
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

def process():
    # load predicate data
    logger.info("loading data... {}", FILE)
    df = pd.read_csv(
        os.path.join(dataDir, FILE), sep="\t", compression="gzip", header=None
    )
    logger.info(df.shape)
    col_names = [
        "query",
        "triple",
        "subject_name",
        "subject_type",
        "subject_id",
        "predicate",
        "object_name",
        "object_type",
        "object_id",
        "localCount",
        "localTotal",
        "globalCount",
        "globalTotal",
        "odds",
        "pval",
        "pmids",
        "gwas-id",
    ]
    df.columns = col_names
    logger.info(df.shape)

    # group
    # keep_cols = ["predicate","subject_name","object_name","subject_type","object_type","subject_id","object_id","id"]

    # need to split subject and object ids by |
    df = (
        df.assign(subject_id=df.subject_id.str.split("|"))
        .explode("subject_id")
        .reset_index(drop=True)
    )
    logger.info(df.shape)
    df = (
        df.assign(object_id=df.object_id.str.split("|"))
        .explode("object_id")
        .reset_index(drop=True)
    )
    logger.info(df.shape)
    logger.info("\n {}", df)

    df["id"] = df["subject_id"] + ":" + df["predicate"] + ":" + df["object_id"]
    keep_cols = [
        "localCount",
        "localTotal",
        "globalCount",
        "globalTotal",
        "odds",
        "pval",
        "gwas-id",
        "id",
    ]
    df = df[keep_cols]
    logger.info(df.shape)
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    df.columns = [
        "localCount",
        "localTotal",
        "globalCount",
        "globalTotal",
        "odds",
        "pval",
        "source",
        "target",
    ]
    logger.info("\n {}", df)

    # deal with inf
    df = df.replace([np.inf, -np.inf], np.nan).dropna(axis=0)
    logger.info(df.shape)

    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
