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

FILE = get_source(meta_id, 1)


def process():
    data = os.path.join(dataDir, FILE)
    df = pd.read_csv(data, sep="\t")
    col_names = [
        "gwas-id",
        "gwas-text",
        "mmi_score",
        "lit-term-name",
        "lit-term-id",
        "mesh",
    ]
    df.columns = col_names
    logger.info(df.head())
    df.drop(["gwas-text", "lit-term-name"], axis=1, inplace=True)
    df.rename(columns={"gwas-id": "source", "lit-term-id": "target"}, inplace=True)
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    logger.info("\n{}", df.head())

    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
