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

FILE1 = get_source(meta_id, 1)
FILE2 = get_source(meta_id, 2)


def process():
    df1 = pd.read_csv(os.path.join(dataDir, FILE1), sep=" ")
    # filter by score
    df1 = df1[df1["combined_score"] >= 700]
    logger.info(df1.shape)
    logger.info("\n {}", df1.head())

    df2 = pd.read_csv(os.path.join(dataDir, FILE2), sep="\t")
    df2.columns = ["species", "uniprot", "protein", "x", "y"]
    df2["uniprot"] = df2["uniprot"].str.split("|", expand=True)[0]
    logger.info(df2.shape)
    logger.info("\n {}", df2.head())

    # merge1
    df_merge = df1.merge(
        df2[["protein", "uniprot"]], left_on="protein1", right_on="protein"
    )
    df_merge.rename(columns={"uniprot": "source"}, inplace=True)
    df_merge.drop("protein", inplace=True, axis=1)
    logger.info(df_merge.shape)
    logger.info("\n {}", df_merge.head())
    # merge2
    df_merge = df_merge.merge(
        df2[["protein", "uniprot"]], left_on="protein2", right_on="protein"
    )
    df_merge.drop("protein", inplace=True, axis=1)
    df_merge.rename(
        columns={"uniprot": "target", "combined_score": "score"}, inplace=True
    )
    df_merge.drop(["protein1", "protein2"], inplace=True, axis=1)

    df_merge.drop_duplicates(inplace=True)
    logger.info(df_merge.shape)
    logger.info("\n {}", df_merge.head())
    create_import(df=df_merge, meta_id=args.name)


if __name__ == "__main__":
    process()
