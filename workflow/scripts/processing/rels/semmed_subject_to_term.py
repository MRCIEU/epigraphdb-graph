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
    # load predicate data
    logger.info("loading data...")
    df = pd.read_csv(
        os.path.join(dataDir, FILE), sep=",", compression="gzip"
    )
    logger.info(df.shape)

    # group

    # need to split subject and object ids by |
    df = (
        df.assign(subject_id=df.subject_id.str.split("|"))
        .explode("subject_id")
        .reset_index(drop=True)
    )
    logger.info(df.shape)
    df = (
        df.assign(object_id=df.object_id.str.split("|"))
        .explode("object_id")
        .reset_index(drop=True)
    )
    logger.info(df.shape)
    logger.info("\n {}", df)

    df["id"] = df["subject_id"] + ":" + df["predicate"] + ":" + df["object_id"]

    keep_cols = ["subject_id", "id"]
    df = df[keep_cols]
    logger.info(df.shape)
    df.drop_duplicates(inplace=True)
    df.columns = ["target", "source"]
    logger.info(df.shape)
    logger.info("\n {}", df.head())

    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
