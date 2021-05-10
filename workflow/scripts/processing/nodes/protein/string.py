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

    p1 = list(df1["protein1"])
    p2 = list(df1["protein2"])
    dfp = pd.DataFrame(p1 + p2)
    dfp.drop_duplicates(inplace=True)
    dfp.columns = ["protein"]
    logger.info(dfp.shape)
    logger.info("\n {}", dfp.head())

    df2 = pd.read_csv(os.path.join(dataDir, FILE2), sep="\t")
    df2.columns = ["species", "uniprot", "protein", "x", "y"]
    df2["uniprot"] = df2["uniprot"].str.split("|", expand=True)[0]
    logger.info(df2.shape)
    logger.info("\n {}", df2.head())

    # merge
    df_merge = dfp.merge(df2, left_on="protein", right_on="protein")
    df_merge = pd.DataFrame(df_merge["uniprot"])
    df_merge.columns = ["uniprot_id"]
    df_merge["name"] = df_merge["uniprot_id"]
    df_merge.drop_duplicates(inplace=True)
    logger.info(df_merge.shape)
    logger.info("\n {}", df_merge.head())
    create_import(df=df_merge, meta_id=args.name)

    constraintCommands = [
        "CREATE CONSTRAINT ON (p:Protein) ASSERT p.uniprot_id IS UNIQUE",
        "CREATE index on :Protein(name);",
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    process()
