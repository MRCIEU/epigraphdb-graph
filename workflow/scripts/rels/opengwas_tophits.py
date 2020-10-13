import os
import sys
import pandas as pd

#################### leave me heare please :) ########################

from workflow.scripts.utils.general import setup

from workflow.scripts.utils.writers import (
    create_constraints,
    create_import,
)

# setup
args, dataDir, dataFiles = setup()
meta_id = args.name

# args = the argparse arguments (name and data)
# dataDir = the path to the working directory for this node/rel
# dataFiles = dictionary of source files specified in data_integration.yml

#######################################################################

FILE = os.path.basename(dataFiles["tophits"])

def gwas():
    df = pd.read_csv(os.path.join(dataDir, FILE), low_memory=False)
    df = df[["id", "rsid", "beta", "p"]].drop_duplicates()

    # edit column names to match schema
    df.rename(columns={"id": "source", "rsid": "target", "p": "pval"}, inplace=True)

    create_import(df=df, meta_id=meta_id)


if __name__ == "__main__":
    gwas()