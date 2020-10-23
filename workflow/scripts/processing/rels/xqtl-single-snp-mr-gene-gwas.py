import os
import gzip
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

def clean_ids(id_list):
    cleaned = []
    for i in id_list:
        if i.isdigit():
            cleaned.append("ieu-a-" + i)
        else:
            cleaned.append(i.lower().replace(":", "-"))
    return cleaned


def process():
    logger.info("Processing data...")

    df = pd.read_csv(os.path.join(dataDir, FILE), sep=",")
    cleaned = clean_ids(df["Gwas.id"].values)
    df["Gwas.id"] = cleaned
    df.rename(
        columns={
            "Gene.ensembl_id": "source",
            "Gwas.id": "target",
            "XQTL_SINGLE_SNP_MR_GENE_GWAS.b": "beta",
            "XQTL_SINGLE_SNP_MR_GENE_GWAS.se": "se",
            "XQTL_SINGLE_SNP_MR_GENE_GWAS.p": "p",
            "XQTL_SINGLE_SNP_MR_GENE_GWAS.qtl_type": "qtl_type",
            "XQTL_SINGLE_SNP_MR_GENE_GWAS.rsid": "rsid",
        },
        inplace=True,
    )

    logger.info(df.shape)
    logger.info("\n{}", df.head())
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    logger.info("\n{}", df.head())
    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    process()
