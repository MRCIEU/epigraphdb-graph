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
    csv_data = []
    with gzip.open(os.path.join(dataDir, FILE)) as json_file:
        data = json.load(json_file)
        for d in data:
            data = [d["child"]["value"], d["parent"]["value"]]
            csv_data.append(data)

    df = pd.DataFrame(csv_data)
    df.drop_duplicates(inplace=True)
    df.rename(columns={0: "source", 1: "target"}, inplace=True)
    logger.info("\n{}", df.head())
    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
