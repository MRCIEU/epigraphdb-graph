import os
import re
import gzip
import json
import sys
import pandas as pd
from loguru import logger

#################### leave me heare please :) ########################

from workflow.scripts.utils.general import setup, get_source, neo4j_connect

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

FILE = get_source(meta_id, 1)


def check():
    driver = neo4j_connect()
    session = driver.session()
    # read data
    data = os.path.join(dataDir, FILE)
    df = pd.read_csv(data, sep="\t")
    print(df.head())

    ens_list = list(set(list(df["ensembl_gene_id"])))
    print(len(ens_list))

    com = """
    match (g:Gene) where g.ensembl_id in {ens_list} return g.ensembl_id;
    """.format(
        ens_list=ens_list
    )

    # print(com)
    result = session.run(com)
    res_df = pd.DataFrame([dict(record) for record in result])
    print(res_df)

    # find missing
    print(set(list(df["ensembl_gene_id"])) - set(list(res_df["g.ensembl_id"])))


def process():
    data = os.path.join(dataDir, FILE)
    df = pd.read_csv(data, sep="\t")

    keep_cols = [
        "ensembl_gene_id",
        "druggability_tier",
        "chr_b37",
        "start_b37",
        "end_b37",
        "description",
        "small_mol_druggable",
        "adme_gene",
        "bio_druggable",
        "hgnc_names",
    ]

    df = df[keep_cols]
    df.rename(
        columns={
            "ensembl_gene_id": "ensembl_id",
            "chr_b37": "chr",
            "start_b37": "start",
            "end_b37": "end",
            "hgnc_names": "name",
        },
        inplace=True,
    )
    logger.info(df.shape)
    logger.info("\n {}", df.head())

    create_import(df=df, meta_id=meta_id)

    # create constraints
    constraintCommands = [
        "CREATE INDEX ON :Gene(druggability_tier);",
        "CREATE CONSTRAINT ON (g:Gene) ASSERT g.ensembl_id IS UNIQUE",
        "CREATE INDEX ON :Gene(name)",
        "CREATE INDEX ON :Gene(chr)",
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    # check()
    process()
