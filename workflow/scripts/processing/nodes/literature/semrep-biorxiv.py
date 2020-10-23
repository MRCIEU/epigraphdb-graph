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

BIO_DATA = get_source(meta_id,1)
BIO_SEM = get_source(meta_id,2)

def merge_data(lit_data, sem_data):
    # load predicate data
    logger.info("loading data...")
    data_df = pd.read_csv(os.path.join(dataDir, lit_data), sep=",", compression="gzip")
    logger.info("\n{}", data_df)

    logger.info("loading semrep data...")
    sem_df = pd.read_csv(os.path.join(dataDir, sem_data), sep=",", compression="gzip")

    logger.info("\n{}", sem_df)
    logger.info(sem_df.shape)

    # merge
    merge_df = data_df.merge(sem_df["doi"], left_on="doi", right_on="doi", how="right")
    # sort by data desc
    merge_df = merge_df.sort_values(by="date", ascending=False)
    # get top one for each doi
    merge_df = merge_df.groupby("doi").head(1)
    logger.info("\n{}", merge_df)
    logger.info(merge_df.shape)

    # format to match Literature requirements
    merge_df["id"] = merge_df["doi"]
    merge_df["year"] = merge_df["date"].str.split("-").str[0]
    # make sure year is an int
    merge_df["year"] = merge_df["year"].astype(str).astype(int)
    merge_df.rename(columns={"server": "issn"}, inplace=True)
    merge_df.drop(
        [
            "published",
            "authors",
            "author_corresponding",
            "author_corresponding_institution",
            "date",
            "version",
            "type",
            "license",
            "category",
            "abstract",
        ],
        axis=1,
        inplace=True,
    )
    logger.info("\n{}", merge_df)
    return merge_df

def process():
    merge = merge_data(BIO_DATA, BIO_SEM)
    logger.info(merge.shape)
    create_import(df=merge, meta_id=args.name)


if __name__ == "__main__":
    process()
