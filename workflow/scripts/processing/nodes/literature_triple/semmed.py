import os
import sys
import pandas as pd
import numpy as np
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


def process():
    # load predicate data
    logger.info("loading data...")
    df = pd.read_csv(
        os.path.join(dataDir, PREDICATION_FILE),
        sep=",",
        compression="gzip",
        low_memory=False,
    )
    logger.info(df.shape)

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
    df["name"] = df["subject_name"] + " " + df["predicate"] + " " + df["object_name"]

    keep_cols = [
        "predicate",
        "subject_name",
        "object_name",
        "subject_type",
        "object_type",
        "subject_id",
        "object_id",
        "id",
    ]
    # keep_cols = ["predicate", "subject_id", "object_id", "id", "name"]

    # df = pd.DataFrame({"count": df.groupby(keep_cols).size()}).reset_index()
    df = df[keep_cols]
    df.drop_duplicates(subset=["id"], inplace=True)
    logger.info(df.shape)
    logger.info("\n {}", df.head())

    # drop nas/rows with empty string
    df.replace("", np.nan, inplace=True)
    df.dropna(inplace=True)

    create_import(df=df, meta_id=meta_id)

    # create constraints
    constraintCommands = [
        "CREATE CONSTRAINT ON (s:LiteratureTriple) ASSERT s.id IS UNIQUE",
        "CREATE INDEX ON :LiteratureTriple(name);",
        "CREATE INDEX ON :LiteratureTriple(subject_id);",
        "CREATE INDEX ON :LiteratureTriple(subject_name);",
        "CREATE INDEX ON :LiteratureTriple(object_id);",
        "CREATE INDEX ON :LiteratureTriple(object_name);",
        "CREATE INDEX ON :LiteratureTriple(predicate);",
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    process()
