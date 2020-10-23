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
    gtex_df = pd.read_csv(data, sep="\t", skiprows=2)

    # create dataframe from tissue name columns
    tissue_names = list(gtex_df.columns)[2:]
    df = pd.DataFrame(tissue_names)
    df.columns = ["id"]
    df['name']=df['id']

    logger.info(df.shape)
    logger.info("\n {}", df.head())
    df.drop_duplicates(inplace=True)
    create_import(df=df, meta_id=meta_id)

    constraintCommands = [
        "CREATE CONSTRAINT ON (t:Tissue) ASSERT t.id IS UNIQUE",
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    process()
