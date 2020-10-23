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

FILE = get_source(meta_id,1)

def process():
    data = os.path.join(dataDir, FILE)
    # some bad rows caused by extra commas so need to skip
    df = pd.read_csv(data, sep=",", skiprows=1, error_bad_lines=False)
    keep_cols = ["Drug"]
    df = df[keep_cols]
    df.drop_duplicates(inplace=True)
    df.columns = ["label"]
    # set label to uppercase
    df["label"] = df["label"].str.upper()
    df["id"] = ""
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
