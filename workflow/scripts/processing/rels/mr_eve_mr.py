import os
import re
import gzip
import json
import sys
import csv
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

def process_data():
    logger.info("Processing mr data...")
    col_names = [
        "source",
        "target",
        "method",
        "nsnp",
        "b",
        "se",
        "ci_low",
        "ci_upp",
        "pval",
        "selection",
        "moescore",
    ]
    data = os.path.join(dataDir, FILE)
    df = pd.read_csv(data,header=None)

    df.columns = col_names
    logger.info(df.shape)
    df.dropna(subset=['pval','se'])
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    logger.info(df.head())
    create_import(df=df, meta_id=meta_id)

    # constraints
    # do we need this ?
    #constraintCommands = [
    #    "match (g:Gwas)-[mr:MR_EVE_MR]->(g2:Gwas) set mr.log10pval = round(-log10(mr.pval));"
    #]
    #create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    process_data()
