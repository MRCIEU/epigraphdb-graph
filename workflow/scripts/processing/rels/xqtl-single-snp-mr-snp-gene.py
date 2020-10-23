import os
import gzip
import sys
import pandas as pd

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
    logger.info("Processing data...")

    FILE = "XQTL_SINGLE_SNP_MR_SNP_GENE.csv"

    df = pd.read_csv(os.path.join(dataDir, FILE), sep=",")
    df.rename(
        columns={"Snp.name": "source", "Gene.ensembl_id": "target",}, inplace=True
    )
    logger.info(df.shape)
    logger.info("\n{}", df.head())
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    logger.info("\n{}", df.head())
    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
