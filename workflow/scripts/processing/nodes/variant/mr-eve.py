import os
import re
import gzip
import json
import sys
import pandas as pd

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

def variant():
    mr_eve_data = "variants.csv.gz"
    print("Reading", mr_eve_data)
    df = pd.read_csv(os.path.join(dataDir, FILE))
    print("Writing...")
    # df = df[["variantId:ID(variant)"]].drop_duplicates()
    df.rename(columns={"pos:INT": "pos", "variantId:ID(variant)": "name"}, inplace=True)
    create_import(df=df, meta_id=meta_id)

    # create constraints
    constraintCommands = [
        "CREATE CONSTRAINT ON (v:Variant) ASSERT v.name IS UNIQUE;",
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    variant()
