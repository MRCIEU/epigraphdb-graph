import os
import sys
import pandas as pd
import gzip
import json
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
    logger.info("Loading efo...")
    data = os.path.join(dataDir, FILE)
    df = pd.read_csv(data)
    df.drop_duplicates(inplace=True)
    df.rename(columns={'efo.id': "target", 'parent_efo.id': "source"}, inplace=True)
    logger.info("\n{}", df.head())
    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
