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

PREDICATION_FILE = get_source(meta_id, 1)
PUB_FILE = get_source(meta_id, 2)


def process():
    # load predicate data
    logger.info("loading predication data...")
    pred_df = pd.read_csv(
        os.path.join(dataDir, PREDICATION_FILE), sep=",", compression="gzip"
    )
    pred_df["PMID"] = pred_df["PMID"].astype(str)

    logger.info("loading citation data...")
    df = pd.read_csv(os.path.join(dataDir, PUB_FILE), sep="\t", compression="gzip")
    df.columns = ["id", "issn", "dp", "edat", "year"]
    df["id"] = df["id"].str.replace("'", "")
    logger.info(df.shape)

    # merge with predication data
    df_merge = df.merge(pred_df["PMID"], left_on="id", right_on="PMID")
    logger.info(df_merge.shape)
    # drop PMID column
    df_merge.drop("PMID", inplace=True, axis=1)
    # make unique
    df_merge.drop_duplicates(inplace=True)
    logger.info(df_merge.shape)

    logger.info(df_merge.shape)
    logger.info("\n {}", df_merge.head())

    create_import(df=df_merge, meta_id=args.name)

    # create constraints
    constraintCommands = [
        "CREATE CONSTRAINT ON (s:Literature) ASSERT s.id IS UNIQUE;",
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    process()
