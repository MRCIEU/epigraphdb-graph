import os
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
    df = pd.read_csv(data, sep="\t", skiprows=2)
    # keep_cols=['gene_id','Description']
    # remove after .
    df["gene_id"] = df["gene_id"].str.split(".", expand=True)[0]

    # melt into long form
    value_cols = list(df.columns[2:])
    df = pd.melt(df, id_vars=["gene_id"], value_vars=value_cols)
    # set source and target
    df.rename(
        columns={"gene_id": "source", "variable": "target", "value": "tpm"},
        inplace=True,
    )
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    df.drop_duplicates(inplace=True)

    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
