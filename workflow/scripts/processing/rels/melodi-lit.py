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

    keep_cols = ["pmids", "gwas-id"]
    df = df[keep_cols]

    # group
    # keep_cols = ["predicate","subject_name","object_name","subject_type","object_type","subject_id","object_id","id"]

    # need to split pmids
    df = (
        df.assign(pmids=df.pmids.str.split(" ")).explode("pmids").reset_index(drop=True)
    )
    logger.info(df.shape)
    logger.info("\n {}", df)

    df.drop_duplicates(inplace=True)
    df.columns = ["target", "source"]
    logger.info(df.shape)
    logger.info("\n {}", df)

    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
