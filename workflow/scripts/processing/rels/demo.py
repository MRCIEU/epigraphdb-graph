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
    # load predicate data
    logger.info("loading data...")
    df = pd.read_csv(os.path.join(dataDir, FILE), sep=",")
    logger.info(df.shape)
    col_names = ["source", "targets"]
    df.columns = col_names
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    logger.info("\n {}", df)

    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
